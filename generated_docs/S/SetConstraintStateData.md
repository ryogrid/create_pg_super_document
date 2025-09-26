# SetConstraintStateData

## Location
[src/backend/commands/trigger.c:3650-3657](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L3650-L3657)

## Overview
SetConstraintStateData is a structure that maintains the complete constraint deferral state for a transaction, tracking both global SET CONSTRAINTS ALL settings and individual trigger-specific constraint states.

## Definition
```c
typedef struct SetConstraintStateData
{
    bool        all_isset;
    bool        all_isdeferred;
    int         numstates;      /* number of trigstates[] entries in use */
    int         numalloc;       /* allocated size of trigstates[] */
    SetConstraintTriggerData trigstates[FLEXIBLE_ARRAY_MEMBER];
} SetConstraintStateData;
```

## Detailed Description
This structure serves as the central container for managing constraint deferral state within a PostgreSQL transaction. It handles both global constraint settings (via SET CONSTRAINTS ALL) and individual constraint trigger settings. The structure is designed as a single palloc'd object for efficient copying and freeing operations. It uses a flexible array member to store variable numbers of individual trigger constraint states, making it memory-efficient while supporting transactions with many constraint triggers.

## Parameters / Member Variables
- `all_isset`: Boolean flag indicating whether SET CONSTRAINTS ALL has been executed in this transaction
- `all_isdeferred`: Boolean flag tracking the state set by SET CONSTRAINTS ALL (true for DEFERRED, false for IMMEDIATE)
- `numstates`: Integer count of active entries in the trigstates[] array
- `numalloc`: Integer representing the allocated size of the trigstates[] array (may be larger than numstates for efficiency)
- `trigstates[]`: Flexible array of SetConstraintTriggerData entries, each storing the deferral state for a specific constraint trigger

## Dependencies
- Functions called/Symbols referenced:
  - SetConstraintTriggerData
  - FLEXIBLE_ARRAY_MEMBER (PostgreSQL macro for variable-length arrays)
- Called from (representative examples):
  - SetConstraintState
  - SetConstraintStateCreate
  - SetConstraintStateAddItem

## Notes and Other Information
- Part of PostgreSQL's intra-transaction constraint state management system
- The structure is designed to be copied efficiently when transaction savepoints are created or rolled back
- Uses flexible array member technique to minimize memory overhead and fragmentation
- Supports both global constraint settings (ALL DEFERRED/IMMEDIATE) and fine-grained per-trigger control
- The all_isset and all_isdeferred fields allow optimization when all constraints are set to the same state
- Located in src/backend/commands/trigger.c as part of the constraint deferral subsystem