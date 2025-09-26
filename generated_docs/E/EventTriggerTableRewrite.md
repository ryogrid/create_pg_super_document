# EventTriggerTableRewrite

## Location
[src/backend/commands/event_trigger.c:1004-1068](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L1004-L1068)

## Overview
EventTriggerTableRewrite fires table_rewrite event triggers when a table undergoes a rewrite operation, providing hooks for monitoring and responding to table restructuring activities.

## Definition
```c
void EventTriggerTableRewrite(Node *parsetree, Oid tableOid, int reason)
```

## Detailed Description
This function is responsible for firing table_rewrite event triggers during table rewrite operations such as ALTER TABLE commands that require physical restructuring of table data. It provides a specialized mechanism for monitoring table modifications that involve data reorganization.

Key features and behaviors:
- Requires currentEventTriggerState to be set, ensuring triggers only fire during active DDL command contexts
- Stores table-specific information (OID and reason) in the event trigger state to make it accessible to trigger functions
- Uses protected execution with PG_TRY/PG_FINALLY to ensure state cleanup even if triggers fail
- Enables pg_event_trigger_table_rewrite_oid function to access the affected table information during trigger execution
- Unlike other event trigger functions, this one calls CommandCounterIncrement after trigger execution to ensure trigger changes are visible to the main command

The function provides essential context information about the table being rewritten and the reason for the rewrite, making it valuable for auditing and monitoring table structure changes.

## Parameters / Member Variables
- `parsetree`: Node pointer representing the parsed DDL command causing the table rewrite
- `tableOid`: Object identifier of the table being rewritten
- `reason`: Integer code indicating the reason for the table rewrite operation

## Dependencies
- Functions called/Symbols referenced:
  - [EventTriggerData](EventTriggerData.md) (struct for trigger context)
  - [EventTriggerCommonSetup](EventTriggerCommonSetup.md) (identifies applicable triggers)
  - EVT_TableRewrite (event type constant)
  - PG_TRY/PG_FINALLY/PG_END_TRY (exception handling)
  - [EventTriggerInvoke](EventTriggerInvoke.md) (executes the triggers)
  - [list_free](../l/list_free.md) (memory cleanup)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md) (ensures visibility)
- Called from (representative examples):
  - [ATRewriteTables](../A/ATRewriteTables.md) (table rewrite processing in ALTER TABLE)

## Notes and Other Information
- Requires active event trigger state from the beginning of the DDL command to function
- Provides table-specific context through table_rewrite_oid and table_rewrite_reason state variables
- Uses exception handling to guarantee proper state cleanup regardless of trigger execution outcome
- The reason parameter provides additional context about why the table rewrite is occurring
- Part of PostgreSQL's comprehensive event trigger system for monitoring table structure changes
- Unlike most other event trigger functions, it increments the command counter after trigger execution
- Enables trigger functions to access rewrite details through pg_event_trigger_table_rewrite_oid