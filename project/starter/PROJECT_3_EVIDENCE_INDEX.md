# AWS AgentCore Classroom --- Project 3

## Evidence Index

**Student:** Swapnil Gaikwad\
**AWS Region:** `us-east-1`\
**Final automated score:** **120/120 (100%)**\
**Verification date:** 6 October 2026

## 1. Purpose

This index maps the Project 3 implementation and verification evidence
to the scored automated tasks.

### Final score

  Task        Area                                               Score
  ----------- ----------------------------------- --------------------
  Task 2      Multi-Agent Orchestration                          40/40
  Task 3      AgentCore Deployment + Guardrails                  20/20
  Task 4      Memory                                             15/15
  Task 5      Bedrock Knowledge Bases                            25/25
  Task 6      Observability                                      20/20
  **Total**                                         **120/120 (100%)**

The test harness accepts `task2`, `task3`, `task4`, `task5`, `task6`,
and `all`. It does not expose a `task1` test.

## 2. Evidence Files

Store screenshots under `Test_Evidences/`.

  ------------------------------------------------------------------------------------------
  ID                      Recommended filename                       Purpose
  ----------------------- ------------------------------------------ -----------------------
  E01                     `Evidence-01-XRay-Service-Map.png`         End-to-end
                                                                     orchestration trace

  E02                     `Evidence-02-AgentCore-Runtime.png`        Runtime
                                                                     deployment/status

  E03                     `Evidence-03-Guardrail.png`                Guardrail configuration

  E04                     `Evidence-04-Memory.png`                   AgentCore Memory

  E05A                    `Evidence-05A-Returns-KB.png`              Returns KB

  E05B                    `Evidence-05B-Shipping-KB.png`             Shipping KB

  E05C                    `Evidence-05C-Warranty-KB.png`             Warranty KB

  E05D                    `Evidence-05D-Parallel-KB-Retrieval.png`   Parallel retrieval

  E06A                    `Evidence-06A-CloudWatch-Logs.png`         CloudWatch logging

  E06B                    `Evidence-06B-XRay-Configuration.png`      X-Ray configuration

  E06C                    `Evidence-06C-Final-Automated-Test.png`    Final 120/120 test
  ------------------------------------------------------------------------------------------

## 3. Task 2 --- Multi-Agent Orchestration

**40/40 --- PASS**

Verified: - Inventory Agent is an Agent and has 3 tools. - Policy Agent
is an Agent and has 1 tool. - Orchestrator Agent is an Agent and has 5
routing tools. - Orchestrator uses `config.ORCHESTRATOR_MODEL_ID` /
Claude Haiku 4.5. - Worker agents use `config.WORKER_MODEL_ID` / Claude
Sonnet 4.5. - Policy retrieval uses `ThreadPoolExecutor`.

Primary supporting evidence: E06C and E01.

## 4. Task 3 --- AgentCore Deployment + Guardrails

**20/20 --- PASS**

Verified: - Guardrail `udacity-agentcore-guardrail` exists. - Content,
PII, topic, and word policies match the specification. - Guardrail
version 1 is published and READY. - AgentCore Runtime is READY. -
Runtime is PUBLIC / HTTP. - Guardrail and Knowledge Base environment
variables are configured.

Supporting evidence: - E02 --- AgentCore Runtime - E03 --- Guardrail

## 5. Task 4 --- Memory

**15/15 --- PASS**

Verified: - Memory: `udacity_agentcore_memory-dSllTrEqOe` - Status:
ACTIVE - Strategy: `SESSION_SUMMARY` / `summaryMemoryStrategy` - Event
expiry: 7 days

Supporting evidence: E04.

## 6. Task 5 --- Bedrock Knowledge Bases

**25/25 --- PASS**

### Returns

-   ID: `0XQWZKPGFO`
-   Status: ACTIVE
-   Embedding: `amazon.titan-embed-text-v2:0`
-   Data-source sync: completed

### Shipping

-   ID: `KOWYSWNPB1`
-   Status: ACTIVE
-   Embedding: `amazon.titan-embed-text-v2:0`
-   Data-source sync: completed

### Warranty

-   ID: `WVMDMRFTWG`
-   Status: ACTIVE
-   Embedding: `amazon.titan-embed-text-v2:0`
-   Data-source sync: completed

The final test also confirmed that all three KBs return passages for a
policy question and that `search_all_policies()` returns combined
results from the three retrievers.

Supporting evidence: E05A--E05D.

## 7. Task 6 --- Observability

**20/20 --- PASS**

Verified: - CloudWatch logging at INFO. - Log group:
`/aws/bedrock/agentcore/udacity-agentcore` - X-Ray tracing enabled. -
Sampling: 100%. - Transaction Search → CloudWatch Logs: ACTIVE. - Three
NovaMart traces were received by X-Ray after live testing.

Supporting evidence: E01, E06A, E06B, E06C.

## 8. End-to-End Runtime Verification

Three live scenarios were exercised:

1.  Return request for `ORD-27176` from `CUST-001`.
2.  Premium return-policy question from `CUST-002`, including parallel
    retrieval from the three KBs.
3.  Out-of-scope discount calculation from `CUST-003`, rejected as
    outside NovaMart customer-service scope.

X-Ray traces were successfully published for the tested scenarios.

## 9. Evidence Completion

Before submission, verify that the actual files exist:

-   [ ] E01
-   [ ] E02
-   [ ] E03
-   [ ] E04
-   [ ] E05A
-   [ ] E05B
-   [ ] E05C
-   [ ] E05D
-   [ ] E06A
-   [ ] E06B
-   [ ] E06C

Do not claim an evidence item is complete until its screenshot/file has
actually been captured.

## 10. Security

Do not commit AWS access keys, secret keys, session tokens, private
credential files, or other secrets.

Final repository checks:

``` bash
git status
git diff --check
```
