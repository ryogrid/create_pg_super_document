# consider_abort_common

## Location
[src/backend/utils/sort/tuplesort.c:1341-1384](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L1341-L1384)

## Overview
Evaluates the effectiveness of abbreviated key optimization and determines whether to abort the abbreviation strategy based on opclass-provided feedback and tuple processing progress.

## Definition
```c
static bool consider_abort_common(Tuplesortstate *state)
```

## Detailed Description
The `consider_abort_common` function implements PostgreSQLs adaptive abbreviated key optimization system. Abbreviated keys are compressed representations of sort keys that enable faster comparisons by reducing memory traffic and CPU cycles. However, this optimization is not always beneficial - in cases where abbreviated keys provide poor selectivity or compression, the overhead may outweigh the benefits.

The function operates on a checkpoint-based evaluation system:
1. **Checkpoint Timing**: Evaluates abbreviation effectiveness at exponentially increasing intervals (`abbrevNext` doubles each time)
2. **Opclass Consultation**: Calls the opclass-provided `abbrev_abort` callback to determine if abbreviation should continue
3. **Strategy Abort**: If abbreviation proves ineffective, switches back to the full comparator and disables abbreviated key processing

When abbreviation is aborted, the function:
- Restores the authoritative (full) comparator function
- Sets `abbrev_converter` to NULL to disable further abbreviation
- Cleans up abbreviation-related function pointers
- Returns `true` to signal that the caller should handle existing abbreviated keys

This adaptive approach ensures that the system automatically falls back to standard comparison when abbreviated keys dont provide sufficient performance benefits, which is particularly important for data with poor abbreviation characteristics.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate structure containing sort configuration and progress information

## Dependencies
- Functions called/Symbols referenced:
  - `TSS_INITIAL`: Tuplesort state constant indicating initial memory-based sorting phase
  - `[Tuplesortstate](../T/Tuplesortstate.md)`: The main sort state structure

- Called from (representative examples):
  - `[tuplesort_puttuple_common](../t/tuplesort_puttuple_common.md)` (src/backend/utils/sort/tuplesort.c:1211)
  - Referenced in `LEADER` context (src/backend/utils/sort/tuplesort.c:458)

## Notes and Other Information
- The function only operates during the `TSS_INITIAL` phase when tuples are being accumulated in memory
- Uses an exponential backoff strategy (`abbrevNext *= 2`) to reduce evaluation frequency as more tuples are processed
- Requires that the sort key has abbreviation support (asserts the presence of `abbrev_converter`, `abbrev_abort`, and `abbrev_full_comparator`)
- The opclass `abbrev_abort` callback receives the current tuple count and sort keys for evaluation
- Returns `true` if abbreviation should be aborted, `false` if it should continue
- Critical for maintaining optimal sort performance across diverse data characteristics
- The abbreviation abort decision is made per-opclass and can consider factors like cardinality, distribution, and compression effectiveness
- Once aborted, abbreviation cannot be re-enabled for the current sort operation
- Part of PostgreSQLs sophisticated cost-based optimization system for sorting operations