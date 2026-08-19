# Verify this run's ASINs actually got DATA ROWS in the product/sales tables,
# not just flags flipped in jungle_scout_sync_status.
# Usage:  .\verify-run.ps1
Get-Content .env | Where-Object { $_ -match '^\s*[A-Z_]+=' } | ForEach-Object {
    $k, $v = $_ -split '=', 2
    Set-Item -Path "env:$($k.Trim())" -Value $v.Trim()
}
$env:PGPASSWORD = $env:DB_STAGING_PASS
& psql -h $env:DB_STAGING_HOST -p $env:DB_STAGING_PORT -U $env:DB_STAGING_USER `
       -d $env:DB_STAGING_NAME -X -f verify-run.sql
