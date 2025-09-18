# GlobalVisTestFor

## Location
src/backend/storage/ipc/procarray.c: 4106 - 4145

## Overview
Returns the appropriate global visibility test state for a given relation, used to determine transaction visibility across the entire cluster.

## Definition


## Detailed Description
This function returns a GlobalVisState pointer that contains visibility horizon information appropriate for the given relation. If rel is NULL, it returns state usable for all relations, which may be more conservative (considering XIDs as not-yet-visible-to-everyone that a relation-specific state would consider visible-to-everyone).

The function determines which type of visibility horizon applies to the relation and returns the corresponding global visibility state. This is essential for making visibility decisions in vacuum operations, HOT pruning, and other cleanup processes that need to determine if tuples can be safely removed.

The function must be called while a snapshot is active or registered to avoid wraparound and other safety issues.

## Parameters / Member Variables
- : The relation for which to get the visibility state. If NULL, returns state usable for all relations.

## Dependencies
- Functions called/Symbols referenced:
  - GlobalVisHorizonKindForRel
  - FullTransactionIdIsValid
- Global visibility state variables:
  - GlobalVisSharedRels
  - GlobalVisCatalogRels  
  - GlobalVisDataRels
  - GlobalVisTempRels
- Called from (representative examples):
  - heap_hot_search_buffer
  - heap_index_delete_tuples
  - heap_page_prune_opt
  - heap_vacuum_rel
  - GlobalVisCheckRemovableFullXid

## Notes and Other Information
- Asserts that RecentXmin is valid, indicating an active snapshot
- The returned state contains definitely_needed and maybe_needed transaction IDs that must be valid
- Different relation types (shared, catalog, data, temp) have different visibility horizons
- Critical for ensuring MVCC correctness during cleanup operations