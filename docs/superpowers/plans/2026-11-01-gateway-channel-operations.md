# Gateway channel operations implementation plan

> Execute inline; use test-driven-development and one final fresh code review.

**Goal:** Replace test-only route summaries with actual New-API model/channel/request-ID operational evidence, and measure controlled sustained relay behavior before rewriting the gateway CV.

**Architecture:** Read-only, bounded management-log snapshots normalize actual channel IDs, request IDs, stream status, gateway timing and quota units. Prometheus output uses window gauges, not repeatedly incremented counters. A sustained workload drives the pinned NGINX/New-API/local-provider stack with explicit request accounting, fixed task inputs and controlled upstream failures. New-API still owns conversion/routing/billing; do not claim upstream adapter authorship.

**Tech:** Python stdlib, actual pinned New-API and NGINX containers, isolated providers.

## Constraints
- No paid model APIs, keys or production capacity claims.
- Preserve logical requests versus attempts and gateway accounting versus invoices.
- Bounded pagination, stable time range, duplicate log suppression; incomplete snapshots explicitly fail.
- Do not export credentials, prompts, IP addresses or raw upstream error text.
- Only résumé intent, core skills and experience may change; preserve RC style and other modules.

## Review focus
- Duplicate/late/paginated logs, failed channel followed by successful fallback.
- Consume logs for interrupted streams are not normal success.
- Missing timing/usage stays unknown; metric window re-scrapes never multiply counts.
- Synthetic load-generator limits must not be called model-service capacity.
- Always remove only this lab's containers and volumes.

## Tasks
- [x] Write failing log normalization / dedup / incomplete-pagination / metric tests, implement read-only collector and Prometheus window output.
- [x] Add actual request-ID tracing, sustained workload and explicit-failure comparison, validate against independent fixture counts and gateway logs.
- [ ] Review, publish/merge under standing authorization, verify affected CI and main.
- [ ] Separate gateway project from Agent runtime in CV, replace weak headline counts with measured relevant results, render and review one page.
