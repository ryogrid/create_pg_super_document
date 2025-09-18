# AfterTriggerEndQuery

## Location
[src/backend/commands/trigger.c:5125-5215](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L5125-L5215)

## Overview
Finalizes query-level trigger processing by executing immediate triggers and transferring deferred triggers to the global list, then cleaning up query-specific resources.

## Definition
```c
void AfterTriggerEndQuery(EState *estate)
```

## Detailed Description
AfterTriggerEndQuery is called after a query has been completely processed to handle all queued trigger events. The function executes all AFTER IMMEDIATE trigger events queued by the query and transfers deferred trigger events to the global deferred-trigger list.

The function processes events in a loop to handle cases where trigger functions queue additional events. It marks events for firing, increments the firing counter, and invokes immediate triggers while preserving deferred ones for later execution. The function includes careful memory management to handle potential reallocation of the query stack during trigger execution.

## Parameters / Member Variables
- `estate`: EState pointer containing executor state information, particularly about target relations needed for trigger execution

## Dependencies
- Functions called/Symbols referenced:
  - [AfterTriggersQueryData](AfterTriggersQueryData.md) (struct type)
  - [afterTriggerMarkEvents](../a/afterTriggerMarkEvents.md) (marks events for processing)
  - CommandId (type for firing counter)
  - [AfterTriggerEventChunk](AfterTriggerEventChunk.md) (event storage structure)
  - [afterTriggerInvokeEvents](../a/afterTriggerInvokeEvents.md) (executes trigger events)
  - [afterTriggerDeleteHeadEventChunk](../a/afterTriggerDeleteHeadEventChunk.md) (memory cleanup)
  - [AfterTriggerFreeQuery](AfterTriggerFreeQuery.md) (releases query-level resources)
- Called from:
  - [CopyFrom](../C/CopyFrom.md) (in src/backend/commands/copyfrom.c:1327)
  - [ExecuteTruncateGuts](../E/ExecuteTruncateGuts.md) (in src/backend/commands/tablecmds.c:2279)
  - [standard_ExecutorFinish](../s/standard_ExecutorFinish.md) (in src/backend/executor/execMain.c:437)
  - [finish_edata](../f/finish_edata.md) (in src/backend/replication/logical/worker.c:716)

## Notes and Other Information
- Must be called BEFORE ExecutorEnd to access EState information about target relations
- Typically called from ExecutorFinish during normal query processing
- Handles memory management carefully due to potential query_stack reallocation during trigger execution
- Implements a loop to process triggers that queue additional events at the same query level
- Separates decision-making (which triggers to fire) from execution to ensure consistent behavior with SET CONSTRAINTS IMMEDIATE
- Decrements query_depth counter to maintain proper nesting level tracking