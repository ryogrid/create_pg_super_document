# AfterTriggerSetState

## Location
src/backend/commands/trigger.c: 5746 - 6060

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
  - GetCurrentTransactionNestLevel (transaction nesting level)
  - SetConstraintStateCreate (create constraint state)
  - SetConstraintStateCopy (copy constraint state for subtransactions)
  - SetConstraintStateAddItem (add trigger to constraint state)
  - get_database_name, LookupExplicitNamespace (name resolution)
  - fetch_search_path, list_make1_oid (search path handling)
  - systable_beginscan, systable_getnext (catalog scanning)
  - afterTriggerMarkEvents, afterTriggerInvokeEvents (trigger firing)
  - PushActiveSnapshot, PopActiveSnapshot (snapshot management)
  - IsSubTransaction (transaction state checking)
- Called from:
  - standard_ProcessUtility (src/backend/tcop/utility.c:939)
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