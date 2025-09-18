# AfterTriggersQueryData

## Location
[src/backend/commands/trigger.c:3920-3926](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L3920-L3926)

## Overview
AfterTriggersQueryData tracks trigger events and related state for a single query execution level, supporting nested query handling and foreign data wrapper integration.

## Definition
```c
struct AfterTriggersQueryData
{
    AfterTriggerEventList events;   /* events pending from this query */
    Tuplestorestate *fdw_tuplestore;    /* foreign tuples for said events */
    List       *tables;         /* list of AfterTriggersTableData, see below */
};
```

## Detailed Description
AfterTriggersQueryData represents the trigger state for a single query execution context within the hierarchical trigger management system. Each query level maintains its own event list, allowing proper isolation of trigger events between nested queries. The structure includes special support for foreign data wrapper (FDW) operations through the fdw_tuplestore field, which stores tuple data for foreign table trigger events. The tables list tracks per-table trigger state for all tables involved in the current query.

## Parameters / Member Variables
- `events`: Collection of trigger events that originated from this specific query level
- `fdw_tuplestore`: Tuple storage for foreign data wrapper table events, NULL for regular tables
- `tables`: List of AfterTriggersTableData structures tracking per-table trigger state

## Dependencies
- Functions called/Symbols referenced:
  - [AfterTriggerEventList](AfterTriggerEventList.md) (for events storage)
  - Tuplestorestate (for FDW tuple storage)
- Called from (representative examples):
  - [AfterTriggersData](AfterTriggersData.md) (query_stack field)
  - [GetAfterTriggersTableData](../G/GetAfterTriggersTableData.md)
  - [AfterTriggerEndQuery](AfterTriggerEndQuery.md)
  - [AfterTriggerFreeQuery](AfterTriggerFreeQuery.md)
  - [AfterTriggerEnlargeQueryState](AfterTriggerEnlargeQueryState.md)
  - cancel_prior_stmt_triggers

## Notes and Other Information
This structure is allocated as part of a dynamic array in AfterTriggersData to support arbitrary nesting depths of query execution. The FDW tuplestore integration is essential for handling triggers on foreign tables, where the actual tuple data may need to be preserved across the network for proper trigger execution. The per-table tracking through the tables list enables efficient management of table-specific trigger optimization and state, particularly important for bulk operations affecting multiple tables.