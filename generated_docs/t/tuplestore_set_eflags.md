# tuplestore_set_eflags

## Location
[src/backend/utils/sort/tuplestore.c:359-382](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplestore.c#L359-L382)

## Overview
Function to set execution capability flags for the primary read pointer of a tuplestore, providing finer control over scanning capabilities than the initial tuplestore creation functions.

## Definition

```c
void
tuplestore_set_eflags(Tuplestorestate *state, int eflags)
```
## Detailed Description
This function allows modification of the execution flags for read pointer 0 after tuplestore creation but before any data insertion. It provides more granular control over scanning capabilities than what is available through the tuplestore_begin_xxx functions. The function updates both the specific read pointer's flags and the global tuplestore flags by combining all read pointers' requirements.

The function enforces strict timing constraints - it must be called while the tuplestore is still in TSS_INMEM status and before any tuples have been inserted. This ensures that the execution strategy can be properly established before tuple storage begins.

## Parameters / Member Variables
- : Pointer to the Tuplestorestate structure to modify
- : Bitmask of execution flags defining the required capabilities:
  - EXEC_FLAG_REWIND: Enables rewinding to the start of the tuplestore
  - EXEC_FLAG_BACKWARD: Enables backward scanning through tuples

## Dependencies
- Functions called/Symbols referenced:
  - elog (error logging function)
  - ERROR (error level constant)
  - TSS_INMEM (tuplestore status indicating memory-only storage)
- Data structures used:
  - [Tuplestorestate](../T/Tuplestorestate.md) (main tuplestore state structure)
- Called from:
  - [ExecInitCteScan](../E/ExecInitCteScan.md) (CTE scan node initialization)
  - [ExecMaterial](../E/ExecMaterial.md) (material node execution)
  - [begin_partition](../b/begin_partition.md) (window aggregation partitioning)

## Notes and Other Information
- Must be called before inserting any data (memtupcount must be 0)
- Can only be called while tuplestore is in TSS_INMEM status
- The function combines eflags from all read pointers to determine the overall tuplestore capabilities
- Setting BACKWARD without REWIND allows backward reading but only to the truncation point
- More flexible than the randomAccess parameter in tuplestore_begin_heap, which sets both REWIND and BACKWARD together
- Violation of timing constraints results in an ERROR-level log message and query termination

## Simplified Source

```c
void
tuplestore_set_eflags(Tuplestorestate *state, int eflags)
{
    int i;

    // Must be called before any data insertion
    if (state->status != TSS_INMEM || state->memtupcount != 0) {
        elog(ERROR, "too late to call tuplestore_set_eflags");
    }

    // Set flags for read pointer 0
    state->readptrs[0].eflags = eflags;

    // Combine flags from all read pointers
    for (i = 1; i < state->readptrcount; i++) {
        eflags |= state->readptrs[i].eflags;
    }

    // Update global tuplestore flags
    state->eflags = eflags;
}
```