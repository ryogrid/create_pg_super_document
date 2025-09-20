# ExecGetTriggerOldSlot

## Location
[src/backend/executor/execUtils.c:1138-1159](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L1138-L1159)

## Overview
Returns a tuple slot for storing the OLD tuple values in trigger processing, creating it lazily if it doesn't already exist.

## Definition

```c
TupleTableSlot *
ExecGetTriggerOldSlot(EState *estate, ResultRelInfo *relInfo)
```
## Detailed Description
This function provides access to a specialized tuple slot used during trigger execution to hold the "OLD" version of a tuple (i.e., the tuple values before an UPDATE or DELETE operation). The function implements lazy initialization - it only creates the slot when first requested and stores it in the ResultRelInfo structure for subsequent reuse.

The OLD slot is essential for trigger processing because triggers often need access to both the previous values (OLD) and new values (NEW) of tuples being modified. This function ensures that the OLD slot is properly initialized with the correct tuple descriptor and table access methods for the relation.

The slot is created in the query's memory context to ensure it persists for the duration of the query execution.

## Parameters / Member Variables
- : The executor state containing query execution context and memory management information
- : Result relation info structure that maintains trigger-related tuple slots and relation metadata

## Dependencies
- Functions called/Symbols referenced:
  -  (creates and initializes a new tuple slot)
  -  (gets appropriate slot callback functions for the table)
- Called from (representative examples):
  -  (src/backend/commands/trigger.c:2703)
  -  (src/backend/commands/trigger.c:2833)
  -  (src/backend/commands/trigger.c:2863)
  -  (src/backend/commands/trigger.c:2992)
  -  (src/backend/commands/trigger.c:3211)
  -  (src/backend/commands/trigger.c:4453, 4470)

## Notes and Other Information
- Uses lazy initialization pattern - slot is only created when first accessed
- The slot is stored in  for reuse across multiple trigger invocations
- Memory context is temporarily switched to  to ensure proper lifetime management
- Commonly used in UPDATE and DELETE triggers where the old tuple values need to be accessible
- Part of PostgreSQL's trigger infrastructure that supports BEFORE, AFTER, and INSTEAD OF triggers
- The tuple descriptor and slot callbacks are obtained from the relation to ensure compatibility with the table's storage format