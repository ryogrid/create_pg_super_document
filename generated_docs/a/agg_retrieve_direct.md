# agg_retrieve_direct

## Location
src/backend/executor/nodeAgg.c: 2194 - 2539

## Overview
agg_retrieve_direct implements non-hashed aggregation processing for PostgreSQL, handling plain aggregation and sorted grouping by directly processing input tuples and managing grouping set boundaries.

## Definition
```c
static TupleTableSlot *
agg_retrieve_direct(AggState *aggstate)
```

## Detailed Description
agg_retrieve_direct is the core function for processing aggregates when not using hash-based grouping (AGG_PLAIN, AGG_SORTED strategies). It manages the complex logic for:

**Multi-phase Processing**: Handles multiple phases of aggregation, particularly for mixed aggregation strategies where it can switch between direct processing and hash table processing.

**Grouping Set Management**: Supports PostgreSQL's GROUPING SETS feature by tracking which grouping sets need to be projected and managing boundaries between different sets.

**Input Processing**: Fetches input tuples from the outer plan, detects group boundaries, and maintains the first tuple of each group for comparison purposes.

**Context Management**: Manages expression contexts for both per-tuple and per-group operations, ensuring proper cleanup and reset of aggregate states between groups.

The function implements a complex state machine that:
1. Determines which grouping sets need to be reset at boundaries
2. Checks for phase completion and transitions to next phase or mixed mode
3. Detects group boundaries by comparing consecutive tuples
4. Initializes and advances aggregate computations
5. Projects final results for each completed group

## Parameters / Member Variables
- `aggstate`: The AggState structure containing all execution state for the aggregate node

## Dependencies
- Functions called/Symbols referenced:
  - ReScanExprContext
  - initialize_phase
  - ResetTupleHashIterator
  - select_current_set
  - agg_retrieve_hash_table
  - ExecQualAndReset
  - fetch_input_tuple
  - TupIsNull
  - ExecCopySlotHeapTuple
  - initialize_aggregates
  - ExecForceStoreHeapTuple
  - lookup_hash_entries
  - advance_aggregates
  - ResetExprContext
  - hashagg_finish_initial_spills
  - ExecQual
  - prepare_projection_slot
  - finalize_aggregates
  - project_aggregates
- Called from (representative examples):
  - ExecAgg (for AGG_PLAIN and AGG_SORTED strategies)

## Notes and Other Information
- This function handles the most complex aggregation scenarios including grouping sets and multi-phase processing
- For mixed aggregation (AGG_MIXED), it can switch to hash table processing by calling agg_retrieve_hash_table
- The function maintains careful state tracking through aggstate->projected_set to handle grouping set boundaries
- Group boundary detection relies on equality functions stored in aggstate->phase->eqfunctions
- Input tuple processing includes special handling for empty input when grouping sets are involved
- The function supports interrupt checking through the main execution loop for long-running aggregations