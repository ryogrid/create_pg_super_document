# EventTriggerCollectAlterDefPrivs

## Location
[src/backend/commands/event_trigger.c:1897-1924](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L1897-L1924)

## Overview
Collects metadata about an ALTER DEFAULT PRIVILEGES command being executed for event trigger processing.

## Definition
```c
void EventTriggerCollectAlterDefPrivs(AlterDefaultPrivilegesStmt *stmt)
```

## Detailed Description
This function is part of PostgreSQL's event trigger system and captures information about ALTER DEFAULT PRIVILEGES commands. When an ALTER DEFAULT PRIVILEGES command is executed, this function creates a CollectedCommand structure containing the command details and stores it in the current event trigger state for later processing by event triggers.

The function operates within the event trigger collection framework and respects the current event trigger context. If event triggers are disabled or collection is inhibited, the function returns early without collecting any information. The function specifically extracts the object type from the statement's action to store in the collected command data.

## Parameters / Member Variables
- `stmt`: Pointer to the AlterDefaultPrivilegesStmt parse tree structure representing the ALTER DEFAULT PRIVILEGES command

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [palloc0](../p/palloc0.md)
  - copyObject
  - [lappend](../l/lappend.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (src/backend/tcop/utility.c:1822)

## Notes and Other Information
- Part of the event trigger collection system that tracks DDL commands for event trigger processing
- Uses SCT_AlterDefaultPrivileges command type for classification
- Memory allocation is performed in the event trigger context to ensure proper lifetime management
- The function creates a deep copy of the parse tree using copyObject to ensure data persistence
- Extracts and stores the object type from stmt->action->objtype for later use by event triggers
- Early return behavior when event triggers are disabled or collection is inhibited prevents unnecessary overhead