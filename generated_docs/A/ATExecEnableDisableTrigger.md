# ATExecEnableDisableTrigger

## Location
[src/backend/commands/tablecmds.c:15604-15621](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L15604-L15621)

## Overview
Executes ALTER TABLE ENABLE/DISABLE TRIGGER commands by delegating to the trigger subsystem and invoking post-alter hooks.

## Definition

```c
static void
ATExecEnableDisableTrigger(Relation rel, const char *trigname,
						   char fires_when, bool skip_system, bool recurse,
						   LOCKMODE lockmode)
```
## Detailed Description
The  function is the execution handler for ALTER TABLE ENABLE/DISABLE TRIGGER commands within the ALTER TABLE infrastructure. It serves as a thin wrapper that delegates the actual trigger manipulation to the specialized trigger subsystem while ensuring proper integration with the ALTER TABLE framework, including invoking necessary post-alter hooks for event triggers and dependency tracking.

## Parameters / Member Variables
- : The relation (table) containing the trigger to be enabled or disabled
- : The name of the trigger to enable or disable
- : Character indicating when the trigger should fire (e.g., 'O' for ORIGIN, 'D' for DISABLED, 'R' for REPLICA, 'A' for ALWAYS)
- : Boolean flag indicating whether to skip system triggers
- : Boolean flag indicating whether to recursively apply to inherited tables
- : The lock mode to use during the operation

## Dependencies
- Functions called/Symbols referenced:
  - [EnableDisableTrigger](../E/EnableDisableTrigger.md)
  - InvokeObjectPostAlterHook
  - RelationRelationId
  - RelationGetRelid
  - InvalidOid
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md) (multiple trigger-related ALTER TABLE subcases)

## Notes and Other Information
- Part of the ALTER TABLE command execution infrastructure
- Supports different trigger firing modes: ORIGIN, DISABLED, REPLICA, ALWAYS
- Handles both individual table operations and recursive operations on inheritance hierarchies
- Integrates with the event trigger system through post-alter hooks
- The actual trigger manipulation logic is implemented in the trigger subsystem (trigger.c)
- Supports skipping system triggers when appropriate

## Simplified Source

```c
static void
ATExecEnableDisableTrigger(Relation rel, const char *trigname,
                          char fires_when, bool skip_system, bool recurse,
                          LOCKMODE lockmode)
{
    // Delegate to the trigger subsystem with InvalidOid for trigoid
    EnableDisableTrigger(rel, trigname, InvalidOid,
                        fires_when, skip_system, recurse,
                        lockmode);

    // Invoke post-alter hooks for event triggers
    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);
}
```