# AfterTriggerBeginQuery

## Location
[src/backend/commands/trigger.c:5105-5124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L5105-L5124)

## Overview
Initializes query-level trigger state by incrementing the query depth counter, preparing for trigger event queuing within a transaction.

## Definition
```c
void AfterTriggerBeginQuery(void)
```

## Detailed Description
AfterTriggerBeginQuery is called just before starting to process a single query within a transaction or subtransaction. The function performs minimal work by simply incrementing the query_depth counter. Most of the actual trigger setup work is deferred until a trigger event is actually queued, making this a lightweight initialization step.

This function is part of PostgreSQL's hierarchical trigger management system that tracks trigger state at both transaction and query levels, allowing for proper nesting and cleanup of trigger contexts.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - afterTriggers global structure (query_depth field)
- Called from:
  - [CopyFrom](../C/CopyFrom.md) (in src/backend/commands/copyfrom.c:804)
  - [ExecuteTruncateGuts](../E/ExecuteTruncateGuts.md) (in src/backend/commands/tablecmds.c:2020) 
  - [standard_ExecutorStart](../s/standard_ExecutorStart.md) (in src/backend/executor/execMain.c:255)
  - [create_edata_for_relation](../c/create_edata_for_relation.md) (in src/backend/replication/logical/worker.c:699)

## Notes and Other Information
- This function uses a lazy initialization approach where heavy setup work is postponed until actually needed
- The query_depth counter enables proper nesting of queries and subqueries within the trigger system
- Must be paired with AfterTriggerEndQuery to properly manage the query stack depth
- Called from various execution contexts including COPY operations, TRUNCATE, executor startup, and logical replication

## Simplified Source

```c
void AfterTriggerBeginQuery(void)
{
    // Increase the query stack depth
    afterTriggers.query_depth++;
}
```