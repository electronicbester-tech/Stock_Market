param(
    [string]$ProjectName = "J.M_Stock_Market",
    [string]$ContactEmail = "you@example.com",
    [int]$TopN = 20,
    [switch]$Excel,
    [switch]$PDF
)

# Activate virtualenv if present
$venv = Join-Path $PSScriptRoot ".venv\Scripts\Activate.ps1"
if (Test-Path $venv) {
    Write-Host "Activating virtualenv..."
    & $venv
}

$script = Join-Path $PSScriptRoot "enhanced_filter.py"
$cmd = "python `"$script`" --project-name `"$ProjectName`" --top-n $TopN --contact-email `"$ContactEmail`""
if ($Excel) { $cmd += " --save-excel" }
if ($PDF) { $cmd += " --save-pdf" }

Write-Host "Running: $cmd"
Invoke-Expression $cmd
