<#
.SYNOPSIS
  Batch converts Shapefiles to GeoJSON using GDAL/ogr2ogr with logging, retries, and options.

.DESCRIPTION
  - Recursively scans a source folder for *.shp
  - Mirrors subfolders in the destination
  - Reprojects to EPSG:4326 (configurable)
  - Optional overwrite, gzip, newline-delimited output
  - Logs all outcomes with a summary

.PARAMETER SourceDir
  Root folder containing Shapefiles.

.PARAMETER DestDir
  Root folder to write GeoJSON outputs, mirroring SourceDir structure.

.PARAMETER TargetEPSG
  EPSG code to reproject to. Default 4326 (WGS84). Set 0 to keep original CRS.

.PARAMETER Overwrite
  Overwrite existing GeoJSON files (otherwise they’re skipped).

.PARAMETER CoordinatePrecision
  Decimal precision for coordinates in GeoJSON (default 6).

.PARAMETER UseGeoJSONSeq
  Output GeoJSONSeq (newline-delimited features). Default false.

.PARAMETER Gzip
  Also produce a .geojson.gz next to the .geojson. Default false.

.PARAMETER Parallel
  Use PowerShell 7+ parallelism. Default false.

.PARAMETER MaxParallel
  Maximum parallel threads when -Parallel is used. Default: half of logical cores, min 2.

.PARAMETER GdalBin
  Optional GDAL bin folder path (where ogr2ogr.exe lives). Prepended to PATH if provided.

.EXAMPLE
  .\Convert-ShapefilesToGeoJSON.ps1 `
    -SourceDir "C:\GIS_Dev\Data\RRC_Data\Shapefiles" `
    -DestDir "C:\GIS_Dev\Data\RRC_Data\GeoJSON" `
    -TargetEPSG 4326 -Overwrite -Gzip -Parallel

#>

[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [ValidateNotNullOrEmpty()]
    [string]$SourceDir,

    [Parameter(Mandatory=$true)]
    [ValidateNotNullOrEmpty()]
    [string]$DestDir,

    [int]$TargetEPSG = 4326,

    [switch]$Overwrite,

    [ValidateRange(0, 15)]
    [int]$CoordinatePrecision = 6,

    [switch]$UseGeoJSONSeq,

    [switch]$Gzip,

    [switch]$Parallel,

    [ValidateRange(1, 256)]
    [int]$MaxParallel = 0,

    [string]$GdalBin = ""
)

# ---- Helpers ---------------------------------------------------------------

function Add-ToPathIfSet([string]$binPath) {
    if ($binPath -and (Test-Path $binPath)) {
        $env:Path = "$binPath;$env:Path"
    }
}

function Test-Exe([string]$exeName) {
    return $null -ne (Get-Command $exeName -ErrorAction SilentlyContinue)
}

function Ensure-Directory([string]$path) {
    if (-not (Test-Path $path)) {
        New-Item -ItemType Directory -Path $path -Force | Out-Null
    }
}

function Compute-RelativePath([string]$root, [string]$full) {
    $rel = $full.Substring($root.Length).TrimStart('\','/')
    return $rel
}

function Build-OgrArgs(
    [string]$outPath,
    [string]$inPath,
    [int]$targetEPSG,
    [int]$coordPrecision,
    [switch]$overwrite,
    [switch]$useGeoJSONSeq
) {
    $args = @()

    # Output format
    if ($useGeoJSONSeq) {
        $args += @("-f", "GeoJSONSeq")
    } else {
        $args += @("-f", "GeoJSON")
        $args += @("-lco", "COORDINATE_PRECISION=$coordPrecision")
    }

    if ($overwrite) { $args += "-overwrite" }

    # Promote single to multi for type consistency
    $args += @("-nlt", "PROMOTE_TO_MULTI")

    # Safeguards: skip bad features instead of failing entire file
    $args += "-skipfailures"

    # Reproject if requested
    if ($targetEPSG -gt 0) {
        $args += @("-t_srs", "EPSG:$targetEPSG")
    }

    # Output then input
    $args += $outPath
    $args += $inPath

    return $args
}

function Gzip-File([string]$path) {
    try {
        if (-not (Test-Path $path)) { return $false }
        $gz = "$path.gz"
        # Overwrite if exists
        if (Test-Path $gz) { Remove-Item -Path $gz -Force -ErrorAction SilentlyContinue }
        $bytes = [System.IO.File]::ReadAllBytes($path)
        $fs = [System.IO.File]::Create($gz)
        $gzip = New-Object System.IO.Compression.GzipStream($fs, [System.IO.Compression.CompressionLevel]::Optimal)
        $gzip.Write($bytes, 0, $bytes.Length)
        $gzip.Close()
        $fs.Close()
        return $true
    } catch {
        Write-Warning "Gzip failed for ${path}: $($_.Exception.Message)"
        return $false
    }
}

# ---- Environment checks ----------------------------------------------------

try { $SourceDir = (Resolve-Path $SourceDir).Path } catch { throw "SourceDir not found: $SourceDir" }
Ensure-Directory -path $DestDir
$DestDir = (Resolve-Path $DestDir).Path

Add-ToPathIfSet -binPath $GdalBin

if (-not (Test-Exe "ogr2ogr")) {
    throw "ogr2ogr.exe not found in PATH. Install GDAL or pass -GdalBin 'C:\OSGeo4W\bin' (or QGIS bin)."
}

# ---- Logging ---------------------------------------------------------------

$timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$logFile = Join-Path $DestDir "convert_geojson_log_$timestamp.txt"
$summary = [System.Collections.Concurrent.ConcurrentBag[string]]::new()

"Start: $(Get-Date -Format 'u')" | Out-File -FilePath $logFile -Encoding UTF8

# ---- Gather inputs ---------------------------------------------------------

$shpFiles = Get-ChildItem -Path $SourceDir -Recurse -Filter *.shp -File
if ($shpFiles.Count -eq 0) {
    "No .shp files found under $SourceDir" | Tee-Object -FilePath $logFile -Append
    Write-Host "No .shp files found under $SourceDir" -ForegroundColor Yellow
    exit 0
}

Write-Host ("Found {0} Shapefile(s)." -f $shpFiles.Count) -ForegroundColor Cyan

# ---- Worker scriptblock ----------------------------------------------------

$worker = {
    param(
        [string]$SourceDir,
        [string]$DestDir,
        [int]$TargetEPSG,
        [int]$CoordinatePrecision,
        [bool]$Overwrite,
        [bool]$UseGeoJSONSeq,
        [bool]$Gzip,
        [string]$LogFile,
        [System.Collections.Concurrent.ConcurrentBag[string]]$Summary,
        [string]$ShpFullName
    )

    try {
        $rel = $ShpFullName.Substring($SourceDir.Length).TrimStart('\','/')
        $baseName = [System.IO.Path]::GetFileNameWithoutExtension($ShpFullName)
        $subDir = Split-Path $rel -Parent
        $outDir = if ([string]::IsNullOrWhiteSpace($subDir)) { $DestDir } else { Join-Path $DestDir $subDir }
        if (-not (Test-Path $outDir)) { New-Item -ItemType Directory -Path $outDir -Force | Out-Null }

        $ext = if ($UseGeoJSONSeq) { ".geojsonl" } else { ".geojson" }
        $outFile = Join-Path $outDir ($baseName + $ext)

        if ((-not $Overwrite) -and (Test-Path $outFile)) {
            $msg = "SKIP (exists): $ShpFullName -> $outFile"
            $msg | Out-File -FilePath $LogFile -Append -Encoding UTF8
            $Summary.Add($msg) | Out-Null
            return
        }

        $args = @()
        if ($UseGeoJSONSeq) {
            $args += @("-f", "GeoJSONSeq")
        } else {
            $args += @("-f", "GeoJSON")
            $args += @("-lco", "COORDINATE_PRECISION=$CoordinatePrecision")
        }
        if ($Overwrite) { $args += "-overwrite" }
        $args += @("-nlt", "PROMOTE_TO_MULTI")
        $args += "-skipfailures"
        if ($TargetEPSG -gt 0) { $args += @("-t_srs", "EPSG:$TargetEPSG") }
        $args += $outFile
        $args += $ShpFullName

        $proc = Start-Process -FilePath "ogr2ogr" -ArgumentList $args -NoNewWindow -PassThru -Wait
        if ($proc.ExitCode -ne 0) {
            $err = "FAIL ($($proc.ExitCode)): $ShpFullName"
            $err | Out-File -FilePath $LogFile -Append -Encoding UTF8
            $Summary.Add($err) | Out-Null
            return
        }

        $ok = "OK: $ShpFullName -> $outFile"
        $ok | Out-File -FilePath $LogFile -Append -Encoding UTF8
        $Summary.Add($ok) | Out-Null

        if ($Gzip -and -not $UseGeoJSONSeq) {
            try {
                $bytes = [System.IO.File]::ReadAllBytes($outFile)
                $gz = "$outFile.gz"
                if (Test-Path $gz) { Remove-Item -Path $gz -Force -ErrorAction SilentlyContinue }
                $fs = [System.IO.File]::Create($gz)
                $gzip = New-Object System.IO.Compression.GzipStream($fs, [System.IO.Compression.CompressionLevel]::Optimal)
                $gzip.Write($bytes, 0, $bytes.Length)
                $gzip.Close(); $fs.Close()
                "GZIP: $gz" | Out-File -FilePath $LogFile -Append -Encoding UTF8
            } catch {
                "GZIP FAIL: $outFile :: $($_.Exception.Message)" | Out-File -FilePath $LogFile -Append -Encoding UTF8
            }
        }

    } catch {
        $e = "ERROR: $ShpFullName :: $($_.Exception.Message)"
        $e | Out-File -FilePath $LogFile -Append -Encoding UTF8
        $Summary.Add($e) | Out-Null
    }
}

# ---- Execute (serial or parallel) -----------------------------------------

$usePwsh7 = $PSVersionTable.PSVersion.Major -ge 7
if ($Parallel -and -not $usePwsh7) {
    Write-Warning "Parallel execution requires PowerShell 7+. Running serially."
    $Parallel = $false
}

if ($Parallel) {
    if ($MaxParallel -le 0) {
        $logical = [Environment]::ProcessorCount
        $MaxParallel = [Math]::Max([Math]::Floor($logical/2), 2)
    }

    $shpFiles.FullName | ForEach-Object -Parallel {
        & $using:worker `
            -SourceDir $using:SourceDir `
            -DestDir $using:DestDir `
            -TargetEPSG $using:TargetEPSG `
            -CoordinatePrecision $using:CoordinatePrecision `
            -Overwrite ([bool]$using:Overwrite) `
            -UseGeoJSONSeq ([bool]$using:UseGeoJSONSeq) `
            -Gzip ([bool]$using:Gzip) `
            -LogFile $using:logFile `
            -Summary $using:summary `
            -ShpFullName $_
    } -ThrottleLimit $MaxParallel
} else {
    foreach ($f in $shpFiles) {
        & $worker `
            -SourceDir $SourceDir `
            -DestDir $DestDir `
            -TargetEPSG $TargetEPSG `
            -CoordinatePrecision $CoordinatePrecision `
            -Overwrite ([bool]$Overwrite) `
            -UseGeoJSONSeq ([bool]$UseGeoJSONSeq) `
            -Gzip ([bool]$Gzip) `
            -LogFile $logFile `
            -Summary $summary `
            -ShpFullName $f.FullName
    }
}

# ---- Summary ---------------------------------------------------------------

"`nCompleted: $(Get-Date -Format 'u')" | Out-File -FilePath $logFile -Append -Encoding UTF8
$okCount   = ($summary | Where-Object { $_ -like 'OK:*' }).Count
$skipCount = ($summary | Where-Object { $_ -like 'SKIP*' }).Count
$failCount = ($summary | Where-Object { $_ -like 'FAIL*' -or $_ -like 'ERROR*' }).Count

$final = "Success: $okCount | Skipped: $skipCount | Failed: $failCount"
$final | Out-File -FilePath $logFile -Append -Encoding UTF8
Write-Host "`n$final" -ForegroundColor Green
Write-Host "Log: $logFile"