# Watch the JungleScout sync job's progress from a second terminal.
# The job logs only to its own stdout, so progress is read from the DB instead.
# Usage:  .\watch-sync.ps1          (refreshes every 15s)
#         .\watch-sync.ps1 -Once    (single snapshot)
param([switch]$Once, [int]$Every = 15)

# Load .env into the process environment
Get-Content .env | Where-Object { $_ -match '^\s*[A-Z_]+=' } | ForEach-Object {
    $k, $v = $_ -split '=', 2
    Set-Item -Path "env:$($k.Trim())" -Value $v.Trim()
}

$env:PGPASSWORD = $env:DB_STAGING_PASS
$p = $env:DB_STAGING_TABLE_PREFIX

$sql = @"
SELECT
  (SELECT count(*) FROM ${p}jungle_scout_sync_status WHERE updated_at > now() - interval '2 hours') AS touched_2h,
  (SELECT count(*) FROM ${p}jungle_scout_sync_status WHERE has_product_data AND updated_at > now() - interval '2 hours') AS with_product,
  (SELECT count(*) FROM ${p}jungle_scout_sync_status WHERE has_sales_data   AND updated_at > now() - interval '2 hours') AS with_sales,
  (SELECT count(*) FROM ${p}jungle_scout_sync_status WHERE error IS NOT NULL AND updated_at > now() - interval '2 hours') AS errored,
  (SELECT max(updated_at) FROM ${p}jungle_scout_sync_status) AS last_write,
  (SELECT round(extract(epoch FROM now() - max(updated_at))) FROM ${p}jungle_scout_sync_status) AS secs_since_write;
"@

do {
    try { Clear-Host } catch {}
    Write-Host "=== sync progress @ $(Get-Date -Format 'HH:mm:ss') ===" -ForegroundColor Cyan
    & psql -h $env:DB_STAGING_HOST -p $env:DB_STAGING_PORT -U $env:DB_STAGING_USER `
           -d $env:DB_STAGING_NAME -X -A -F ' | ' -c $sql
    $job = Get-Process -Name job, main -ErrorAction SilentlyContinue
    if ($job) { Write-Host "`njob process: ALIVE (pid $($job.Id -join ','))" -ForegroundColor Green }
    else      { Write-Host "`njob process: not found (may be running as 'go run' temp binary)" -ForegroundColor Yellow }
    if (-not $Once) { Start-Sleep -Seconds $Every }
} while (-not $Once)
