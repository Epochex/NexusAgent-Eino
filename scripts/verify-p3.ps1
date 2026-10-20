[CmdletBinding()]
param(
    [string]$ModuleCache = '',
    [switch]$SkipInfrastructureStartup,
    [ValidateSet('Backend','Frontend')]
    [string[]]$Checks = @('Backend','Frontend')
)
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location -LiteralPath $repoRoot
try {
    if ($Checks -contains 'Frontend' -and $IsWindows) {
        $preview = Get-CimInstance Win32_Process -Filter "Name = 'node.exe'" | Where-Object {
            $_.CommandLine -and $_.CommandLine.Contains($repoRoot, [StringComparison]::OrdinalIgnoreCase) -and $_.CommandLine -match 'vite'
        }
        if ($preview) { throw 'Stop the Vite preview for this checkout before npm ci; Windows locks its native modules.' }
    }
    if ($Checks -contains 'Backend') {
    if (-not $SkipInfrastructureStartup) {
        & docker compose -f backend/deployments/docker-compose.acceptance.yaml up -d --wait --wait-timeout 180
        if ($LASTEXITCODE -ne 0) { throw "Acceptance infrastructure startup failed ($LASTEXITCODE)" }
    }
    $mounts = @('--mount', "type=bind,source=$repoRoot,target=/workspace",
        '--mount', 'type=volume,source=nexus-eino-go-build,target=/root/.cache/go-build')
    if ($ModuleCache) {
        $cachePath = (Resolve-Path -LiteralPath $ModuleCache).Path
        $mounts += @('--mount', "type=bind,source=$cachePath,target=/go/pkg/mod")
    } else {
        $mounts += @('--mount', 'type=volume,source=nexus-eino-acceptance-go-mods,target=/go/pkg/mod')
    }
    # Dedicated test database and public fixture credentials; no user model keys.
    $dockerArgs = @('run', '--rm', '--network', 'nexus-eino-acceptance_default', '--cpus=4', '--memory=4g',
        '--env', 'NX_TEST_DATABASE_DSN=postgres://postgres@postgres:5432/nexus_acceptance_test?sslmode=disable',
        '--env', 'NX_TEST_REDIS_ADDR=redis:6379', '--env', 'NX_TEST_MILVUS_ADDR=milvus:19530',
        '--env', 'NX_TEST_MINIO_ADDR=minio:9000') + $mounts + @('-w', '/workspace/backend', 'golang:1.26.0', 'sh', '-c',
        'go vet ./... && go test -race ./... -count=1 -timeout=180s && go test -race -tags "integration infrastructure" ./internal/execution ./internal/service ./internal/api ./internal/repo ./internal/ingestion ./internal/vector ./internal/memory/long_term ./internal/tool ./internal/a2a ./internal/capacity -count=1 -timeout=300s && go build ./cmd/server ./cmd/worker ./cmd/cli ./cmd/migrate')
    & docker @dockerArgs
    if ($LASTEXITCODE -ne 0) { throw "Backend acceptance failed ($LASTEXITCODE)" }
    }
    if ($Checks -contains 'Frontend') {
        Push-Location -LiteralPath (Join-Path $repoRoot 'frontend')
        try {
            & npm.cmd ci
            if ($LASTEXITCODE -ne 0) { throw "Frontend dependency install failed ($LASTEXITCODE)" }
            & npm.cmd run typecheck
            if ($LASTEXITCODE -ne 0) { throw "Frontend typecheck failed ($LASTEXITCODE)" }
            & npm.cmd run build
            if ($LASTEXITCODE -ne 0) { throw "Frontend build failed ($LASTEXITCODE)" }
        } finally { Pop-Location }
    }
    Write-Output "Passed checks: $($Checks -join ', '). Real-model and manual browser results are separate."
} finally { Pop-Location }
# Keep the acceptance volumes and logs. Do not clean unrelated validation state.
