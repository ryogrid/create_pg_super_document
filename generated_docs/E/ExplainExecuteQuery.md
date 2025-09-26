# ExplainExecuteQuery

## Location
[src/backend/commands/prepare.c:568-683](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/prepare.c#L568-L683)

## Overview
Implements the 'EXPLAIN EXECUTE' utility statement, providing execution plan analysis for prepared statements with support for parameter evaluation and detailed performance metrics.

## Definition
```c
void ExplainExecuteQuery(ExecuteStmt *execstmt, IntoClause *into, ExplainState *es,
                        const char *queryString, ParamListInfo params,
                        QueryEnvironment *queryEnv)
```

## Detailed Description
This function handles the EXPLAIN EXECUTE command, which allows users to see the execution plan of a prepared statement without actually executing it. The function performs several key operations:

1. **Parameter Resolution**: If the prepared statement has parameters, it evaluates them using the provided parameter values
2. **Plan Retrieval**: Fetches the cached plan from the prepared statement, potentially triggering a replan if needed
3. **Performance Monitoring**: Tracks planning time, memory usage, and buffer usage when requested
4. **Plan Explanation**: Iterates through each statement in the plan list and explains them using ExplainOnePlan or ExplainOneUtility

The function supports both regular EXPLAIN EXECUTE and EXPLAIN CREATE TABLE AS EXECUTE variants. It ensures proper resource management by creating and cleaning up executor states and releasing cached plan references.

## Parameters / Member Variables
- `execstmt`: The EXECUTE statement containing the prepared statement name and parameter values
- `into`: NULL unless doing EXPLAIN CREATE TABLE AS EXECUTE, specifies the target table
- `es`: ExplainState structure controlling output format and analysis options
- `queryString`: The query string of the EXPLAIN EXECUTE command (not the original PREPARE)
- `params`: Parameter list information from the calling context
- `queryEnv`: Query environment for parameter resolution and execution context

## Dependencies
- Functions called/Symbols referenced:
  - [FetchPreparedStatement](../F/FetchPreparedStatement.md)
  - [make_parsestate](../m/make_parsestate.md)
  - [CreateExecutorState](../C/CreateExecutorState.md)
  - [EvaluateParams](EvaluateParams.md)
  - [GetCachedPlan](../G/GetCachedPlan.md)
  - [ExplainOnePlan](ExplainOnePlan.md)
  - [ExplainOneUtility](ExplainOneUtility.md)
  - [ExplainSeparatePlans](ExplainSeparatePlans.md)
  - [FreeExecutorState](../F/FreeExecutorState.md)
  - [ReleaseCachedPlan](../R/ReleaseCachedPlan.md)
- Data structures used:
  - [PreparedStatement](../P/PreparedStatement.md)
  - [CachedPlan](../C/CachedPlan.md)
  - [ExecuteStmt](ExecuteStmt.md)
  - [ExplainState](ExplainState.md)
  - [IntoClause](../I/IntoClause.md)
  - [ParamListInfo](../P/ParamListInfo.md)
  - [QueryEnvironment](../Q/QueryEnvironment.md)
- Called from (representative examples):
  - [ExplainOneUtility](ExplainOneUtility.md)

## Notes and Other Information
- Only supports fixed-result cached plans; variable-result plans are explicitly rejected
- Properly handles memory context switching for memory usage analysis
- Supports buffer usage tracking when enabled in ExplainState
- Creates a transient executor state for parameter evaluation that persists until the end of the function
- The function acquires and releases a transient refcount on the cached plan to ensure proper resource management
- Performance metrics (planning time, memory usage, buffer usage) are collected and passed to the individual plan explanation functions
- Multiple statements in a plan list are separated with appropriate separators in the output