# transientrel_startup

## Location
[src/backend/commands/matview.c:466-491](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/matview.c#L466-L491)

## Overview
transientrel_startup is an executor startup function that initializes a DestReceiver for writing tuples to a transient relation, setting up the necessary state and configuration for bulk insert operations.

## Definition

```c
static void
transientrel_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
```
## Detailed Description
This function serves as the startup callback for a DestReceiver that handles writing tuples to a transient relation, typically used in materialized view operations. It initializes the DR_transientrel state structure by opening the target transient relation and configuring bulk insert parameters for optimal performance. The function sets up frozen tuple insertion with FSM (Free Space Map) skipping to maximize insertion speed for temporary data.

## Parameters / Member Variables
- `self`: DestReceiver pointer cast to DR_transientrel containing the transient relation OID and state
- `operation`: Integer indicating the type of operation being performed (not used in this implementation)
- `typeinfo`: TupleDesc describing the structure of tuples to be inserted (not used in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](table_open.md)
  - [GetCurrentCommandId](../G/GetCurrentCommandId.md)
  - [GetBulkInsertState](../G/GetBulkInsertState.md)
  - RelationGetTargetBlock
  - TABLE_INSERT_SKIP_FSM (constant)
  - TABLE_INSERT_FROZEN (constant)
- Called from (representative examples):
  - [CreateTransientRelDestReceiver](../C/CreateTransientRelDestReceiver.md) (callback assignment)

## Notes and Other Information
- Uses NoLock when opening the transient relation since it's a temporary relation with controlled access
- Sets TABLE_INSERT_SKIP_FSM flag to bypass free space map for performance optimization
- Sets TABLE_INSERT_FROZEN flag to insert frozen tuples, eliminating the need for subsequent vacuum operations
- Includes an assertion check to ensure the relation hasn't been written to previously, maintaining the assumption of a clean transient relation
- The output_cid is obtained with 'true' parameter to GetCurrentCommandId, indicating it should be used for command tracking