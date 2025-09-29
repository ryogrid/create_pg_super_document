# postquel_getnext

## Location
[src/backend/executor/functions.c:876-910](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/functions.c#L876-L910)

## Overview
Executes one execution state node either to completion or until the first result row is produced, returning completion status.

## Definition
```c
static bool
postquel_getnext(execution_state *es, SQLFunctionCachePtr fcache)
```

## Detailed Description
This function drives execution of a single query within a SQL function, handling both utility and regular commands differently. Utility commands are processed via ProcessUtility and always run to completion. Regular commands use ExecutorRun with different execution modes: lazy evaluation mode fetches only one tuple at a time, while normal mode runs to completion. The function determines completion by checking if no tuples were requested (count == 0) or no tuples were processed.

## Parameters / Member Variables
- `es`: Pointer to execution_state structure containing the query descriptor and execution context
- `fcache`: Pointer to SQL function cache containing function metadata including returnsSet flag

## Dependencies
- Functions called/Symbols referenced:
  - [ProcessUtility](../P/ProcessUtility.md)
  - [ExecutorRun](../E/ExecutorRun.md)
  - ForwardScanDirection (scan direction constant)
  - PROCESS_UTILITY_QUERY (utility context constant)
- Called from (representative examples):
  - [fmgr_sql](../f/fmgr_sql.md)

## Notes and Other Information
- Returns true if execution ran to completion, false if stopped early (lazy evaluation)
- Utility commands always return true as they cannot be interrupted
- For regular commands, uses count=1 in lazy mode to fetch single tuples, count=0 for completion
- Respects returnsSet flag to determine when to allow early stopping in lazy mode
- Uses es_processed count from executor estate to detect when no tuples were produced
- Protects function cache's parse tree when processing utility statements

## Simplified Source

```c
static bool
postquel_getnext(execution_state *es, SQLFunctionCachePtr fcache)
{
    bool result;

    if (es->qd->operation == CMD_UTILITY) {
        // Process utility commands (DDL, etc.) to completion
        ProcessUtility(es->qd->plannedstmt,
                      fcache->src,
                      true,  /* protect function cache's parsetree */
                      PROCESS_UTILITY_QUERY,
                      es->qd->params,
                      es->qd->queryEnv,
                      es->qd->dest,
                      NULL);
        result = true;  // Utility commands always complete
    } else {
        // For regular queries: lazy mode gets 1 tuple, normal mode runs to completion
        uint64 count = (es->lazyEval) ? 1 : 0;

        ExecutorRun(es->qd, ForwardScanDirection, count,
                   !fcache->returnsSet || !es->lazyEval);

        // Complete if we ran to end or got no tuples
        result = (count == 0 || es->qd->estate->es_processed == 0);
    }

    return result;
}
```