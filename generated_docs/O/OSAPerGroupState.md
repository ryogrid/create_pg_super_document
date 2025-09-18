# OSAPerGroupState

## Location
src/backend/utils/adt/orderedsetaggs.c: 92 - 104

## Overview
OSAPerGroupState is a structure that holds per-group state data for ordered-set aggregates in PostgreSQL. It maintains group-specific information including the sorting state and accumulation progress for each aggregate group.

## Definition


## Detailed Description
OSAPerGroupState represents the per-group portion of PostgreSQL's ordered-set aggregate state management. While OSAPerQueryState contains information shared across all groups in a query, OSAPerGroupState maintains data that is specific to each individual aggregate group being processed.

This structure serves as the internal-type transition state datum that is returned to nodeAgg.c. It manages the sorting process for each group, tracking the number of rows accumulated and the current state of the sort operation. The structure works in conjunction with OSAPerQueryState to provide efficient processing of ordered-set aggregates like percentile_cont, percentile_disc, mode, and hypothetical-set functions.

## Parameters / Member Variables
- : Pointer to the associated OSAPerQueryState containing shared query-level information
- : Memory context containing per-group data for this specific group
- : Tuplesort object used for accumulating and sorting data rows for this group
- : Count of normal (non-hypothetical) rows that have been inserted into the sort state
- : Boolean flag indicating whether tuplesort_performsort() has been called to finalize the sort

## Dependencies
- Functions called/Symbols referenced:
  - [OSAPerQueryState](OSAPerQueryState.md)
  - Tuplesortstate
- Called from (representative examples):
  - [ordered_set_startup](../o/ordered_set_startup.md)
  - [ordered_set_shutdown](../o/ordered_set_shutdown.md)
  - [ordered_set_transition](../o/ordered_set_transition.md)
  - [ordered_set_transition_multi](../o/ordered_set_transition_multi.md)
  - [percentile_disc_final](../p/percentile_disc_final.md)
  - [percentile_cont_final_common](../p/percentile_cont_final_common.md)
  - [percentile_disc_multi_final](../p/percentile_disc_multi_final.md)
  - [percentile_cont_multi_final_common](../p/percentile_cont_multi_final_common.md)
  - [mode_final](../m/mode_final.md)
  - [hypothetical_rank_common](../h/hypothetical_rank_common.md)
  - [hypothetical_dense_rank_final](../h/hypothetical_dense_rank_final.md)

## Notes and Other Information
- This structure is allocated in a per-group memory context that is separate from the per-query context
- The sort_done flag is used to ensure that tuplesort_performsort() is only called once per group, as it transitions the sort state from accumulation mode to read mode
- The number_of_rows field excludes hypothetical rows, which are handled separately in hypothetical-set aggregates
- This structure is part of PostgreSQL's two-level state management system for ordered-set aggregates, optimizing memory usage and enabling aggregate merging when possible