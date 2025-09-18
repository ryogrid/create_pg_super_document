# vac_close_indexes

## Location
src/backend/commands/vacuum.c: 2362 - 2382

## Overview
Releases the resources acquired by vac_open_indexes, specifically closing index relations and optionally releasing their locks.

## Definition


## Detailed Description
This function is the cleanup counterpart to vac_open_indexes. It iterates through an array of index relations in reverse order, closing each index using index_close() and then freeing the memory allocated for the index relation array. The function provides flexibility in lock management by allowing the caller to specify whether to release locks (by passing a specific LOCKMODE) or keep them (by passing NoLock).

## Parameters / Member Variables
- : The number of indexes in the Irel array
- : Array of Relation pointers representing the opened index relations to be closed
- : The lock mode to use when closing indexes; pass NoLock to retain existing locks

## Dependencies
- Functions called/Symbols referenced:
  - [index_close](../i/index_close.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [heap_vacuum_rel](../h/heap_vacuum_rel.md)
  - [do_analyze_rel](../d/do_analyze_rel.md)
  - [parallel_vacuum_main](../p/parallel_vacuum_main.md)

## Notes and Other Information
- The function safely handles NULL input by returning early if Irel is NULL
- Indexes are closed in reverse order (from nindexes-1 down to 0)
- The function frees the Irel array memory using pfree() after closing all indexes
- This is a cleanup function that should be called to properly release resources acquired during vacuum operations