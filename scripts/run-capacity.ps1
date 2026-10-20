[CmdletBinding()]
param(
    [ValidateSet('BootstrapOnly','Pilot','Grid','Grid500','Baseline','All','Faults')]
    [string]$Suite = 'BootstrapOnly',
    [string]$Go = 'go',
    [switch]$SkipBuild,
    [switch]$SkipInfrastructureStartup
)
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$binRoot = Join-Path $repoRoot 'output/capacity-bin'
$runID = (Get-Date -Format 'yyyyMMdd-HHmmss') + '-' + [Guid]::NewGuid().ToString('N').Substring(0,8)
$runRoot = Join-Path $repoRoot "output/capacity/$runID"
New-Item -ItemType Directory -Path $binRoot,$runRoot -Force | Out-Null
$native = Join-Path $binRoot $(if ($IsWindows) { 'loadtest.exe' } else { 'loadtest-native' })
Push-Location -LiteralPath $repoRoot
try {
    if (-not $env:NX_TEST_DATABASE_DSN) {
        $env:NX_TEST_DATABASE_DSN = 'postgres://postgres@127.0.0.1:55449/nexus_acceptance_test?sslmode=disable'
    }
    if (-not $SkipInfrastructureStartup) {
        & docker compose -f backend/deployments/docker-compose.acceptance.yaml up -d --wait --wait-timeout 180
        if ($LASTEXITCODE -ne 0) { throw 'Acceptance infrastructure startup failed' }
    }
    if (-not $SkipBuild) {
        $moduleCache = (& $Go env GOMODCACHE).Trim()
        if ($LASTEXITCODE -ne 0) { throw 'Go toolchain unavailable' }
        & docker run --rm --cpus=4 --memory=4g `
            --env GIT_CONFIG_COUNT=1 --env GIT_CONFIG_KEY_0=safe.directory --env GIT_CONFIG_VALUE_0=/workspace `
            --mount "type=bind,source=$repoRoot,target=/workspace" `
            --mount "type=bind,source=$moduleCache,target=/go/pkg/mod" `
            --mount 'type=volume,source=nexus-eino-go-build,target=/root/.cache/go-build' `
            --mount "type=bind,source=$binRoot,target=/out" -w /workspace/backend `
            golang@sha256:fb612b7831d53a89cbc0aaa7855b69ad7b0caf603715860cf538df854d047b84 `
            sh -c 'CGO_ENABLED=0 go build -trimpath -o /out/server ./cmd/server && CGO_ENABLED=0 go build -trimpath -o /out/worker ./cmd/worker && CGO_ENABLED=0 go build -trimpath -o /out/loadtest ./cmd/loadtest'
        if ($LASTEXITCODE -ne 0) { throw 'Linux application/peer build failed' }
        Push-Location -LiteralPath (Join-Path $repoRoot 'backend')
        try {
            & $Go build -trimpath -o $native ./cmd/loadtest
            if ($LASTEXITCODE -ne 0) { throw 'Native load driver build failed' }
        } finally { Pop-Location }
        Copy-Item -LiteralPath (Join-Path $repoRoot 'backend/configs') -Destination $binRoot -Recurse -Force
    }
    $fixtureFile = Join-Path $runRoot 'fixture.json'
    & $native -mode prepare -fixture $fixtureFile -migrations (Join-Path $repoRoot 'backend/migrations')
    if ($LASTEXITCODE -ne 0) { throw 'Fresh isolated fixture preparation failed' }
    $prepared = Get-Content -LiteralPath $fixtureFile -Raw | ConvertFrom-Json
    $env:NX_CAPACITY_SCHEMA = $prepared.schema
    & docker compose -f backend/deployments/docker-compose.capacity.yaml up -d --force-recreate
    if ($LASTEXITCODE -ne 0) { throw 'Capacity application startup failed' }
    $ready = $false
    $deadline = (Get-Date).AddSeconds(60)
    while ((Get-Date) -lt $deadline) {
        try {
            $health = Invoke-WebRequest -Uri 'http://127.0.0.1:18090/healthz' -TimeoutSec 1
            if ($health.StatusCode -eq 200) { $ready = $true; break }
        } catch {}
        Start-Sleep -Milliseconds 200
    }
    if (-not $ready) { throw 'Actual API did not become healthy' }
    & $native -mode bootstrap -fixture $fixtureFile
    if ($LASTEXITCODE -ne 0) { throw 'Actual login/model/MCP/Agent/workflow API bootstrap failed' }
    $manifest = [ordered]@{
        suite = $Suite; schema = $prepared.schema; started_at = (Get-Date).ToUniversalTime().ToString('o')
        source_revision = (& git rev-parse HEAD).Trim()
        source_dirty = [bool]((& git status --porcelain) -join '')
        platform_cpu_limit = 4; platform_memory_gib_limit = 8
        model_delay_ms = 250; tool_delay_ms = 100
        binaries = @('server','worker','loadtest') | ForEach-Object {
            $item = Get-FileHash -LiteralPath (Join-Path $binRoot $_) -Algorithm SHA256
            [ordered]@{ name = $_; sha256 = $item.Hash.ToLowerInvariant() }
        }
    }
    $manifest | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $runRoot 'manifest.json') -Encoding utf8
    Write-Output "Bootstrap verified against actual services. Artifact directory: $runRoot"
    if ($Suite -ne 'BootstrapOnly') {
        & (Join-Path $PSScriptRoot 'measure-capacity.ps1') -Fixture $fixtureFile -Suite $Suite -Driver $native -OutputDirectory (Join-Path $runRoot 'campaign')
    }
} finally { Pop-Location }
# Preserve this project's containers, fixture schemas and evidence. No broad cleanup.
