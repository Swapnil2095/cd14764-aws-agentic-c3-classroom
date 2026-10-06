# AWS AgentCore Classroom --- Project 3

## Completion Report

**Student:** Swapnil Gaikwad\
**Region:** `us-east-1`\
**Verification date:** 6 October 2026\
**Final score:** **120/120 (100%)**

## Executive Summary

Project 3 has been implemented and verified against the supplied
automated test suite.

Final command:

``` bash
python tests/test_agent.py all
```

Final result:

``` text
Score: 120/120 pts (100%)

🎉 Perfect score! All tasks complete.
```

## Task Results

  Task        Area                                Result
  ----------- ----------------------------------- ------------------
  Task 2      Multi-Agent Orchestration           40/40 PASS
  Task 3      AgentCore Deployment + Guardrails   20/20 PASS
  Task 4      Memory                              15/15 PASS
  Task 5      Bedrock Knowledge Bases             25/25 PASS
  Task 6      Observability                       20/20 PASS
  **Total**                                       **120/120 PASS**

## Deployment

The AgentCore Runtime was verified as:

``` text
Status: READY
Protocol: HTTP
Network: PUBLIC
```

The Guardrail was verified as:

``` text
udacity-agentcore-guardrail
Version 1
READY
```

## Memory

``` text
Memory: udacity_agentcore_memory-dSllTrEqOe
Status: ACTIVE
Strategy: SESSION_SUMMARY
Event expiry: 7 days
```

## Knowledge Bases

``` text
Returns:  0XQWZKPGFO
Shipping: KOWYSWNPB1
Warranty: WVMDMRFTWG
```

All three were verified ACTIVE, using `amazon.titan-embed-text-v2:0`,
with completed synchronization.

## Observability

CloudWatch log group:

``` text
/aws/bedrock/agentcore/udacity-agentcore
```

X-Ray:

``` text
Sampling: 100%
Transaction Search → CloudWatch Logs: ACTIVE
```

After live testing, the automated test detected three NovaMart traces.

## End-to-End Tests

### Return workflow

``` text
Customer: CUST-001
Order: ORD-27176
Request: I want to return my wireless headphones from order ORD-27176
```

The workflow initialized the session and routed through the specialist
agents involved in the return process.

### Policy workflow

``` text
Customer: CUST-002
Question: What is the return policy for premium customers?
```

The Policy Agent retrieved information from the three Knowledge Bases in
parallel.

### Out-of-scope workflow

``` text
Customer: CUST-003
Question: How much would 5 items at $29.99 be with a 10% discount?
```

The request was rejected as outside NovaMart customer-service scope.

## Final Status

**Technical implementation:** COMPLETE\
**Automated verification:** COMPLETE\
**Score:** 120/120 (100%)\
**AWS visual evidence:** Capture/verify before final submission\
**Repository final check:** Pending evidence upload\
**Udacity submission:** Pending final repository/evidence review
