# ExecSetupTransitionCaptureState

## Location
[src/backend/executor/nodeModifyTable.c:3864-3892](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L3864-L3892)

## Overview
Sets up the state needed for collecting transition tuples for AFTER triggers, creating transition capture states for both normal operations and ON CONFLICT UPDATE scenarios.

## Definition
```c
static void ExecSetupTransitionCaptureState(ModifyTableState *mtstate, EState *estate)
```

## Detailed Description
The ExecSetupTransitionCaptureState function initializes the transition capture mechanism required for AFTER triggers to access OLD and NEW transition tables. It creates a primary transition capture state for the main operation and, for INSERT operations with ON CONFLICT UPDATE clauses, creates an additional transition capture state specifically for the UPDATE portion of the conflict resolution. The function uses MakeTransitionCaptureState to create these capture states based on the target relation's trigger descriptor, relation OID, and operation type.

## Parameters / Member Variables
- `mtstate`: Pointer to ModifyTableState that will store the created transition capture states
- `estate`: Pointer to EState containing the execution state (parameter present but not directly used in current implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [MakeTransitionCaptureState](../M/MakeTransitionCaptureState.md)
  - RelationGetRelid
  - [ModifyTable](../M/ModifyTable.md) (plan structure)
  - CMD_INSERT, CMD_UPDATE (operation constants)
  - ONCONFLICT_UPDATE (conflict action constant)
- Called from (representative examples):
  - [ExecInitModifyTable](ExecInitModifyTable.md) (at src/backend/executor/nodeModifyTable.c:4496)

## Notes and Other Information
- This function is called during the initialization phase of ModifyTable execution to set up transition capture before any tuples are processed
- The mt_transition_capture field stores the main transition capture state used for regular operations
- The mt_oc_transition_capture field stores the additional transition capture state specifically for ON CONFLICT UPDATE scenarios
- Transition capture states are essential for statement-level AFTER triggers that need access to OLD and NEW transition tables
- The function only sets up capture for the directly targeted relation, not for any potential child relations in inheritance hierarchies
- Located in src/backend/executor/nodeModifyTable.c:3864-3892