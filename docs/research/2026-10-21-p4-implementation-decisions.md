# P4 实施决策与延后项

所有实施及审查取舍按原记录顺序保留；下方原文含实施当时的状态，最终状态见[发布验收](2026-10-21-release-validation.md)。

关键代价：共享 PG 协议要求所有进程遵循同一准入、围栏与版本规则，回滚旧进程可能突破额度；受控模型、同机 VM 和有限时长不能证明真实模型质量、跨机容量或无限稳定性；未知写入需要人工核对。500 SSE 使用受限初始建连后保持同样的测量窗口，不能宣称验证过500个同时握手。

<details>
<summary>完整决策原始记录（按时间排序）</summary>

### 决策 1

Task1 Ruling: use a shared DB active-run VIEW uniting existing fenced workflow_runs leases with explicit Chat leases, rather than a second mirrored workflow quota ledger or triggers. Acquire+workflow claim share schema-scoped advisory locking and DB authoritative policy; approval/backoff/finish release via existing status/lease fields, no hidden release mutation needed. A2A calls Chat once and must not doublecount. Provider lease lifetime includes entire actual stream and heartbeat loss cancels request; cannot promise opaque remote HTTP side effects stop physically when TCP cancels. Cost: active-run view predicates and shared lock must remain consistent across all admission paths; tests deliberately mix Chat/workflow/processes/expiry.

### 决策 2

Task1 Ruling: shared PG policy rows (initial run32/owner4, provider16/owner4, outstanding1000/owner100) are authority, not independent per-process configuration. Local worker/HTTP bounds still limit each process; background model work with no actor uses explicit shared owner0 quota group, not systemuser identity/access. Changes via versioned policy/explicit operational control must not silently diverge across nodes. Implement/test defaults then loadfixture can set8/32/64 under controlled test schema. Fair ClaimSupported picks least-recently-served eligible owner, skips saturated owners. Cached Enqueue request remains readable atqueuecapacity; new work returns429. These mechanisms are not implemented yet.

### 决策 3

Task1 policy Ruling: adminGET /api/v1/admin/capacity returnsDBsampledsharedusage+policies; PUT /:scope requiresversion/fullvalidatedlimits, CAS409,invalid400,currentadminrolecheck. AdmissiontakesSHAREpolicyrowlock, so update linearizes againstpendingadmissions. Shrinkgrandfatherexistingwork/blocknew, TTLnewleasesonly. Providerpoolcounts aggregateacrossallorigins (notcomparedtooneoriginlimit); providerpolicylimitappliestoeachoriginhash. No rawendpoint/ownerIDs inprocessmetriclabels. Processcountercapacity_operations_total distinguishesgranted/reject/error/renew_lost/release_error; DBsnapshotprovidesglobalusage. APItestRED404; twoimplementationfailures preserved(GORMScanstructrelationship,thenignoredfieldreset), correctedscan/assign; validGREENapi2.890s includingactualHTTPshrink/stale/invalid/oldadminJWTrevocation. CI+verifyscriptnewcapacitypackageincluded.

### 决策 4

Task2 Ruling (before implementation): approved short workload is2model250ms+1tool100ms. Use80%real durable LLM→MCP readonlytool→LLM workflows and20%actualChat/EinoADK FunctionCalling→MCPtool→finalSSE. No agentDSLnode exists; do notinventdurableADKnodeforbenchmark. Totalarrivalratecoversboth, reportworkflowfirst-result separatelyfromChatfirst-token. 16actualactors(global64withowner4), currentJWTDBchecksremain. Maintain50/200/500actualworkfloweventSSEsubscriptions, reportactivevswaitingseedconnections, rotateclosedterminalsubscriptions; exactstage grid36cells plus200/32/10for30min and5minidle×3cycles. Slots areconfiguredcapacity, reportobservedpeak separately. Openarrivalgeneratorrecordsallscheduled/offered/skipped/rejected/accepted/terminal andindependentPGcounts; controlledpeerCPU/RAMexternalto4CPU8GiBserver+workercombined.

### 决策 5

Task2 isolation Ruling: PDF parsing moves to separately deployed private docparser sidecar with hard DockerCPU/memory/pid constraints, nohostmount/noDockerSocket/nonroot/read-onlyroot/dropcaps; oneboundedparseatonce, tokenauth,32MiBinput/explicitoutputbound,10sprocessdeadline. HTTPdaemon spawns its own separatechildmode for each PDF, kills child on timeout/disconnect, containerrestartonOOM. Worker PDF failsclosed ifparserunconfigured; plain/docx/html remain boundedexistingcode. Never claim GOMEMLIMIT is a hard memory sandbox. Acceptance must inspectactualcontainerlimits and kill/timeout parser whilebackend survives; otherprocesssamecontaineris processisolation buthardmemoryboundaryis whole dedicatedparsercontainer. No publicAPIendpoint and no userURL/tokeninrequest.

### 决策 6

Task2 Ruling: execute the full 36-cell grid for explicit 30s/cell, then exact 200 SSE/32 slots/10 new requests per second for 30 minutes. Recovery trends use that baseline plus two additional 5-minute load cycles, each followed by 5 minutes idle (3 drains total). This does not claim three 30-minute loads; cost if wrong: longer repetition may reveal a slower leak and requires extending the campaign. Keep all raw failures and final independent PG/peer accounting.

### 决策 7

Task2 profile Ruling: retain partial pre-optimization grid (50 SSE all12 cells + failed200 SSE cells; stage interrupted only after actual CPU profile). Do not waste remaining original cells after confirmed hot path. Re-run ALL36cells plus exact30min/cycles on corrected binary/fresh schema. Cost: old500SSE comparison unavailable; no old500capacityclaim. Profile server15s14.18CPU seconds, SSE RunEvents62.34% cumulative and GetWorkflowRun45.49%; DBpool32/32inuse/201325waits and PG~292%CPU at200SSE. Plan optimization justified by evidence: replace multi-query full-detail polling with bounded owner-scoped event/status read at one PG snapshot, preserve cursor-retention and terminal-drain semantics.

### 决策 8

Task3 sandbox independent gap: upstream ignored configured memory/CPU/pool, mutableimage, unboundedlogbuffersbeforetruncation. TestSandboxRejects... actualRED5cases allaccepted. Implementvalidatedhardbounds/immutablepreloadedimage/localpool/nonrootreadonlydropcapsnoneNetwork/envwhitelist/128MiBtmpfs/pid64/nofile256/filesize32MiB/logrotation+bounedbuffers/in-containerGNUtimeout/boundedDockercleanupbyknownrandomname. ProductionrequireexplicitremoteTLS env andfailstartupwhenenabledinvalid; actualimageinspect3s. Ruling: bounded tmpfs explicitly exec to support Go programs (otherwise actualGoRun forkpermissiondeniedRED); no hostfilemount/socket, no network, rootreadonly; costifwrong: any codeallowedwithincontainercanexecutebinariesinthisboundedtmpfs, samecontainersecurityboundary. Dedicateddaemonrequirementisoperatorcontract, TLSguardcannotprovephysicaldedication.

### 决策 9

Task2 Ruling: keep all failed original500raw; actualdiagnostic500/32/1 recorded196setup_dial_error/554opened, zero load/drain errors. Bound initial headers32 atonce, still all500actualbeforearrivalwindow and20ssetupdeadline; setup failures remain total/passcriterion. Underlying errno unknown, do not inventOSpolicycause. Cost: ramp differs from burst-handshake stress; reportramp separately. Corrected12cells running48708, appbinaryd8925c0 frozen; onlydriverchanged.

### 决策 10

Final: Ruling: reviewer setaside realLLM/embedding/cost/providercapacity — no config, explicitlydeferred peruser; costifwrongunmeasuredanswerquality/vendorlimits. CrosshostSLA/infiniteuptime/three30minloads — onlysameVM/finiteobservations/first30+two5minloadsproved; costifwrongunderestimatedrealdeploymentlimits. DedicatedTLSsandboxphysicalindependence/productionbackuprestore — operatorverificationstillrequired, nativeisolationtestnotdeploymentproof; costifwrongprivilegeorrestorefailure. Othermigrationdowncompatibility — only013tested, prohibitextrapolation; costifwrongdataloss. Latestfullsuite/imageboot/remoteCI — authorwillfinishallbeforemergeclaim, notinferredfromreview.

</details>

## 延后小问题

Final: minor (deferred): browser favicon.ico 404 produces two console entries; functional chat/workflow walkthrough succeeds. No frontend runtime error observed; retain as cosmetic limitation, no extra polish scope.
