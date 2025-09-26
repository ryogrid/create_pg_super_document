# leader_takeover_tapes

## Location
[src/backend/utils/sort/tuplesort.c:3107-3165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L3107-L3165)

## Overview
Creates a tapeset for the leader process from worker tapes in parallel tuplesort operations, allowing the leader to take over sorting operations after all workers have completed.

## Definition

```c
static void
leader_takeover_tapes(Tuplesortstate *state)
```
## Detailed Description
This function is a critical component of PostgreSQL's parallel tuplesort implementation. It is called by the leader process after all worker processes have finished their sorting tasks. The function transforms the leader's Tuplesortstate to make it appear as if it had performed serial external sorting, when in reality the sorting was done by parallel workers.

The function performs several key operations:
1. Validates that all workers have finished by checking the shared state
2. Initializes the tape state for the number of participants
3. Creates a logical tapeset from the shared fileset
4. Sets up output tapes by importing each worker's tape
5. Configures the state to indicate that run building is complete

This design allows the leader to seamlessly continue with the merge phase of external sorting, treating the worker-generated runs as if they were locally generated.

## Parameters / Member Variables
- : Pointer to the leader's Tuplesortstate structure that needs to be configured to take over from workers

## Dependencies
- Functions called/Symbols referenced:
  - LEADER (macro to check if this is the leader process)
  - inittapestate (initializes tape state structures)
  - LogicalTapeSetCreate (creates a logical tape set)
  - LogicalTapeImport (imports worker tapes into the tapeset)
  - Tuplesortstate (the main sort state structure)
  - Sharedsort (shared state between parallel processes)
  - LogicalTape (individual tape structure)
  - TSS_BUILDRUNS (status indicating run building phase)

- Called from (representative examples):
  - tuplesort_performsort (main sorting function)

## Notes and Other Information
- This function is only called by the leader process in parallel tuplesort operations
- All worker processes must have completed before this function is called, or it will raise an ERROR
- The function assumes exactly one run per worker, which is guaranteed by the parallel tuplesort design
- After this function completes, the leader can proceed with the merge phase as if it had performed serial external sorting
- The number of tapes created equals the number of participants, making this suitable for parallel merging operations