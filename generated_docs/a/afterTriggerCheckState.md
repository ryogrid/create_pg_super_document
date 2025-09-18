# afterTriggerCheckState

## Location
src/backend/commands/trigger.c: 4041 - 4085

## Overview
Determines whether a deferrable trigger event is currently in deferred state based on constraint settings and trigger properties.

## Definition
```c
static bool afterTriggerCheckState(AfterTriggerShared evtshared)
```

## Detailed Description
This function implements the logic for determining the deferral state of trigger events in PostgreSQL's constraint system. It evaluates whether a trigger should be executed immediately or deferred until the end of the transaction, based on several factors:

1. **Non-deferrable triggers**: Always returns false for triggers that cannot be deferred
2. **Explicit SET CONSTRAINTS**: Checks if SET CONSTRAINTS was used to override the default behavior for specific triggers
3. **Global SET CONSTRAINTS ALL**: Checks if SET CONSTRAINTS ALL was used to set a global deferral state
4. **Default trigger state**: Falls back to the trigger's initial deferral setting if no explicit constraints were set

The function is crucial for PostgreSQL's constraint enforcement mechanism, allowing users to control when constraint checks are performed within a transaction.

## Parameters / Member Variables
- `evtshared`: Shared trigger event data containing trigger OID and event flags that determine deferability and initial deferral state

## Dependencies
- Functions called/Symbols referenced:
  - AFTER_TRIGGER_DEFERRABLE (flag check)
  - AFTER_TRIGGER_INITDEFERRED (flag check)
  - SetConstraintState (type)
  - AfterTriggerShared (type)
  - afterTriggers (global state structure)
- Called from (representative examples):
  - afterTriggerMarkEvents

## Notes and Other Information
- The function is static and used internally within the after-trigger system
- Returns true if the trigger is in deferred state, false if it should execute immediately
- Handles the hierarchy of constraint settings: specific trigger settings override global settings
- Non-deferrable triggers (normal AFTER ROW triggers and NOT DEFERRABLE constraints) always return false
- Part of PostgreSQL's transaction-level constraint management system
- Essential for implementing SQL standard deferrable constraints
- The function examines both per-trigger and global constraint states set by SET CONSTRAINTS commands