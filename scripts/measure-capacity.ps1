[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Fixture,
    [ValidateSet('Pilot','Grid','Grid500','Baseline','All','Faults')][string]$Suite='Pilot',
    [string]$Driver,
    [string]$OutputDirectory
)
$ErrorActionPreference='Stop'
$repoRoot=Split-Path -Parent $PSScriptRoot
if (-not $Driver) { $Driver=Join-Path $repoRoot 'output/capacity-bin/loadtest.exe' }
$Fixture=(Resolve-Path -LiteralPath $Fixture).Path
if (-not $OutputDirectory) { $OutputDirectory=Join-Path (Split-Path -Parent $Fixture) ('campaign-'+(Get-Date -Format 'yyyyMMdd-HHmmss')) }
New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null
if (-not $env:NX_TEST_DATABASE_DSN) { $env:NX_TEST_DATABASE_DSN='postgres://postgres@127.0.0.1:55449/nexus_acceptance_test?sslmode=disable' }
$campaignID=[Guid]::NewGuid().ToString('N').Substring(0,8)
$binaryRoot=Split-Path -Parent $Driver
$frozenManifestPath=Join-Path (Split-Path -Parent $Fixture) 'manifest.json'
$frozenRevision=if(Test-Path -LiteralPath $frozenManifestPath){(Get-Content -LiteralPath $frozenManifestPath -Raw|ConvertFrom-Json).source_revision}else{$null}
[ordered]@{
    campaign_id=$campaignID; source_revision=(& git -C $repoRoot rev-parse HEAD).Trim()
    driver_hash=(Get-FileHash -LiteralPath $Driver -Algorithm SHA256).Hash
    app_server_hash=(Get-FileHash -LiteralPath (Join-Path $binaryRoot 'server') -Algorithm SHA256).Hash
    app_worker_hash=(Get-FileHash -LiteralPath (Join-Path $binaryRoot 'worker') -Algorithm SHA256).Hash
    app_frozen_source_revision=$frozenRevision
    primary_mix='80% durable LLM-MCP-LLM / 20% Eino ADK Chat-MCP-final SSE'
    sse='Fixed slots, initial ramp batches32; all subscriptions must exist before arrival window. Chat replaces one idle slot; workflow subscriptions return to waiting fixtures. Actual counts, ramp duration and setup/phase errors are reported.'
    model_delay_ms=250; tool_delay_ms=100
    platform='server 2 CPU/4GiB + worker 2 CPU/4GiB; peer, driver and all infrastructure external'
    source_dirty=[bool]((& git -C $repoRoot status --porcelain) -join '')
} | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $OutputDirectory 'manifest.json') -Encoding utf8
$results=@()
function Invoke-CapacityStage([string]$Name,[int]$SSE,[int]$Slots,[int]$Rate,[string]$Duration,[string]$Idle='0s') {
    $stageName="$campaignID-$Name"
    $reportPath=Join-Path $OutputDirectory "$Name.json"
    & $Driver -mode stage -fixture $Fixture -stage $stageName -output $reportPath -sse $SSE -slots $Slots -rate $Rate -duration $Duration -idle $Idle
    if ($LASTEXITCODE -ne 0) { throw "Stage $Name runner failed; preserve all raw evidence" }
    $report=Get-Content -LiteralPath $reportPath -Raw | ConvertFrom-Json
    $script:results += [ordered]@{name=$Name;report=$reportPath;passed=$report.passed;offered=$report.actual_wire_requests;gaps=$report.gaps}
    $script:results | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $OutputDirectory 'index.json') -Encoding utf8
}
Push-Location -LiteralPath $repoRoot
try {
    if ($Suite -eq 'Pilot') { Invoke-CapacityStage pilot 50 8 1 '30s' }
    if ($Suite -in @('Grid','Grid500','All')) {
        $sseValues=if($Suite -eq 'Grid500'){@(500)}else{@(50,200,500)}
        foreach ($sse in $sseValues) { foreach ($slots in @(8,32,64)) { foreach ($rate in @(1,5,10,20)) {
            Invoke-CapacityStage "grid-s$sse-c$slots-r$rate" $sse $slots $rate '30s'
        } } }
    }
    if ($Suite -in @('Baseline','All')) {
        Invoke-CapacityStage baseline-30m 200 32 10 '30m' '5m'
        Invoke-CapacityStage recovery-2 200 32 10 '5m' '5m'
        Invoke-CapacityStage recovery-3 200 32 10 '5m' '5m'
    }
    if ($Suite -eq 'Faults') {
        # Kill/restart only this explicitly named acceptance worker. The stage
        # independently audits retained PG work after lease expiry/recovery.
        $reportPath=Join-Path $OutputDirectory 'worker-kill.json'
        $job=Start-ThreadJob -ScriptBlock {
            param($Executable,$FixtureFile,$Report,$Name,$DSN)
            $env:NX_TEST_DATABASE_DSN=$DSN
            & $Executable -mode stage -fixture $FixtureFile -stage $Name -output $Report -sse 200 -slots 32 -rate 10 -duration 60s
            if ($LASTEXITCODE -ne 0) { throw 'Fault stage driver failed' }
        } -ArgumentList $Driver,$Fixture,$reportPath,"$campaignID-worker-kill",$env:NX_TEST_DATABASE_DSN
        # Wait for the first measured resource record, rather than racing the
        # fixture setup or measuring the wrong phase.
        $deadline=(Get-Date).AddMinutes(4)
        while (-not (Test-Path -LiteralPath ($reportPath+'.resources.jsonl'))) {
            if ((Get-Date) -gt $deadline -or $job.State -in @('Failed','Completed')) { Receive-Job $job; throw 'Fault workload never started' }
            Start-Sleep -Milliseconds 200
        }
        Start-Sleep -Seconds 10
        & docker kill --signal KILL nexus-eino-capacity-worker-1
        if ($LASTEXITCODE -ne 0) { throw 'Owned worker kill failed' }
        Start-Sleep -Seconds 5
        & docker start nexus-eino-capacity-worker-1
        if ($LASTEXITCODE -ne 0) { throw 'Owned worker restart failed' }
        $job | Wait-Job | Receive-Job
        if ($job.State -ne 'Completed') { throw 'Fault load did not complete' }
        # Expected at-least-once readonly calls may exceed the clean 2+1 count;
        # retain the raw failed clean-target result instead of relabeling it.
    }
    Write-Output "Capacity observations: $OutputDirectory"
} finally { Pop-Location }
