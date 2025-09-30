# AfterTriggerSetState

## Location
[src/backend/commands/trigger.c:5746-6060](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L5746-L6060)

## Overview
AfterTriggerSetState executes the SET CONSTRAINTS utility command, managing the deferred/immediate state of constraint triggers and firing any newly immediate triggers retroactively.

## Definition

```c
void
AfterTriggerSetState(ConstraintsSetStmt *stmt)
```
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
  - [PushActiveSnapshot](../P/PushActiveSnapshot.md), PopActiveSnapshot (snapshot management)
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

## Simplified Source

```c
void AfterTriggerSetState(ConstraintsSetStmt *stmt) {
    int my_level = GetCurrentTransactionNestLevel();

    // Initialize state if needed
    if (afterTriggers.state == NULL)
        afterTriggers.state = SetConstraintStateCreate(8);

    // Save current state for subtransaction rollback
    if (my_level > 1 && afterTriggers.trans_stack[my_level].state == NULL) {
        afterTriggers.trans_stack[my_level].state =
            SetConstraintStateCopy(afterTriggers.state);
    }

    if (stmt->constraints == NIL) {
        // Handle "SET CONSTRAINTS ALL ..."
        afterTriggers.state->numstates = 0;
        afterTriggers.state->all_isset = true;
        afterTriggers.state->all_isdeferred = stmt->deferred;
    } else {
        // Handle "SET CONSTRAINTS constraint-name [, ...]"
        List *conoidlist = NIL;
        List *tgoidlist = NIL;

        // Look up constraint names and collect constraint OIDs
        foreach(lc, stmt->constraints) {
            RangeVar *constraint = lfirst(lc);

            // Search for constraint in appropriate schema(s)
            // Add found deferrable constraints to conoidlist
            // Error if constraint not found or not deferrable
        }

        // Find descendant constraints (for partitioned tables)
        foreach(lc, conoidlist) {
            // Scan for child constraints and add to list
        }

        // Find triggers implementing these constraints
        foreach(lc, conoidlist) {
            // Look up triggers for each constraint
            // Add deferrable triggers to tgoidlist
        }

        // Update trigger states
        foreach(lc, tgoidlist) {
            Oid tgoid = lfirst_oid(lc);

            // Find existing state or add new state
            SetConstraintStateAddItem(state, tgoid, stmt->deferred);
        }
    }

    // If setting to IMMEDIATE, fire any previously deferred events
    if (!stmt->deferred) {
        bool snapshot_set = false;

        // Mark and fire events that are now immediate
        while (afterTriggerMarkEvents(events, NULL, true)) {
            if (!snapshot_set) {
                PushActiveSnapshot(GetTransactionSnapshot());
                snapshot_set = true;
            }

            // Fire the events
            if (afterTriggerInvokeEvents(events, firing_id, NULL, !IsSubTransaction()))
                break;
        }

        if (snapshot_set)
            PopActiveSnapshot();
    }
}
```