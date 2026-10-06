# AWS AgentCore Classroom --- Project 3

## Final Test Results

**Date:** 6 October 2026\
**Region:** `us-east-1`\
**Command:** `python tests/test_agent.py all`

## Final Score

``` text
120/120 pts
100%
🎉 Perfect score! All tasks complete.
```

## Task 2 --- Multi-Agent Orchestration

**40/40**

-   `build_inventory_agent()` returns Agent --- PASS
-   Inventory Agent has 3 tools --- PASS
-   `build_policy_agent()` returns Agent --- PASS
-   Policy Agent has 1 tool --- PASS
-   `build_orchestrator_agent()` returns Agent --- PASS
-   Orchestrator has 5 routing tools --- PASS
-   Orchestrator uses Claude Haiku 4.5 --- PASS
-   Worker agents use Claude Sonnet 4.5 --- PASS
-   Parallel retrievers use `ThreadPoolExecutor` --- PASS

## Task 3 --- AgentCore Deployment + Guardrails

**20/20**

-   Guardrail exists --- PASS
-   Guardrail policies match --- PASS
-   Guardrail version 1 published and READY --- PASS
-   AgentCore Runtime READY --- PASS
-   Runtime PUBLIC / HTTP --- PASS
-   Guardrail + KB environment variables configured --- PASS

Guardrail:

``` text
udacity-agentcore-guardrail
```

## Task 4 --- Memory

**15/15**

-   Memory ACTIVE --- PASS
-   `SESSION_SUMMARY` strategy --- PASS
-   7-day event expiry --- PASS

Memory:

``` text
udacity_agentcore_memory-dSllTrEqOe
```

## Task 5 --- Bedrock Knowledge Bases

**25/25**

### Returns

``` text
0XQWZKPGFO
ACTIVE
amazon.titan-embed-text-v2:0
Completed sync
```

### Shipping

``` text
KOWYSWNPB1
ACTIVE
amazon.titan-embed-text-v2:0
Completed sync
```

### Warranty

``` text
WVMDMRFTWG
ACTIVE
amazon.titan-embed-text-v2:0
Completed sync
```

The test confirmed that all three Knowledge Bases return passages for a
policy question and that `search_all_policies()` returns combined
results.

## Task 6 --- Observability

**20/20**

CloudWatch:

``` text
INFO logging enabled
/aws/bedrock/agentcore/udacity-agentcore
```

X-Ray:

``` text
Tracing enabled
Sampling: 100%
Transaction Search → CloudWatch Logs: ACTIVE
```

Final trace verification:

``` text
3 NovaMart trace(s) received by X-Ray
```

## Live End-to-End Test

Tested:

``` text
I want to return my wireless headphones from order ORD-27176
```

``` text
What is the return policy for premium customers?
```

``` text
How much would 5 items at $29.99 be with a 10% discount?
```

X-Ray traces were successfully published for the tested scenarios.

## Note on task1

`python tests/test_agent.py task1` is not a project failure. The test
harness reports:

``` text
Unknown argument: task1
Usage: python test_agent.py [task2|task3|task4|task5|task6|all]
```

Therefore `task1` is simply not an available test argument.

## Final Conclusion

All scored Project 3 tasks passed:

``` text
120/120 pts (100%)
```
