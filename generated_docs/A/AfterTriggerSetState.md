# AfterTriggerSetState

## Location
[src/backend/commands/trigger.c:5746-6060](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L5746-L6060)

## Overview
AfterTriggerSetState executes the SET CONSTRAINTS utility command, managing the deferred/immediate state of constraint triggers and firing any newly immediate triggers retroactively.

## Definition


## Detailed Description
This function implements the SQL SET CONSTRAINTS command, which allows changing the deferred/immediate status of deferrable constraints. It supports both "SET CONSTRAINTS ALL" and "SET CONSTRAINTS constraint-name [, ...]" syntax.

For "SET CONSTRAINTS ALL", it resets all previous constraint states and sets a global deferred/immediate flag. For named constraints, it looks up the constraints by name (supporting schema-qualified names and search path resolution), finds associated deferrable triggers, and updates their state.

The function also handles partitioned table constraints by finding descendant constraints. When constraints are set to IMMEDIATE, it retroactively fires any previously deferred trigger events that are now immediate, as required by SQL99.

Key behaviors:
- Saves current state when entering subtransactions for rollback capability  
- Validates that constraints are deferrable before allowing state changes
- Supports cross-schema constraint lookup with search path resolution
- Handles constraint inheritance in partitioned tables
- Retroactively fires events when switching from DEFERRED to IMMEDIATE

## Parameters / Member Variables
- : ConstraintsSetStmt structure containing:
  - : List of constraint names (NIL for ALL)
  - : Boolean indicating desired deferred/immediate state

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTransactionNestLevel](../G/GetCurrentTransactionNestLevel.md) (transaction nesting level)
  - [SetConstraintStateCreate](../S/SetConstraintStateCreate.md) (create constraint state)
  - [SetConstraintStateCopy](../S/SetConstraintStateCopy.md) (copy constraint state for subtransactions)
  - [SetConstraintStateAddItem](../S/SetConstraintStateAddItem.md) (add trigger to constraint state)
  - [get_database_name](../g/get_database_name.md), LookupExplicitNamespace (name resolution)
  - [fetch_search_path](../f/fetch_search_path.md), list_make1_oid (search path handling)
  - [systable_beginscan](../s/systable_beginscan.md), systable_getnext (catalog scanning)
  - [afterTriggerMarkEvents](../a/afterTriggerMarkEvents.md), afterTriggerInvokeEvents (trigger firing)
  - PushActiveSnapshot, PopActiveSnapshot (snapshot management)
  - [IsSubTransaction](../I/IsSubTransaction.md) (transaction state checking)
- Called from:
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (src/backend/tcop/utility.c:939)
  - TRIGGER_DISABLED (src/include/commands/trigger.h:283)

## Notes and Other Information
- Implements SQL99 requirement for retroactive firing of immediate constraints
- Handles both global (ALL) and individual constraint state management
- Supports schema search path resolution for unqualified constraint names
- Manages constraint state persistence across subtransaction boundaries
- Uses catalog scanning to resolve constraint names to trigger OIDs
- Automatically handles constraint inheritance in partitioned tables
- Ensures snapshot consistency when firing retroactive triggers
- Prevents cross-database constraint references as per PostgreSQL limitations