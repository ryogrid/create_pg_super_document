# make_canonical_pathkey

## Location
[src/backend/optimizer/path/pathkeys.c:55-105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L55-L105)

## Overview
Creates or retrieves a canonical PathKey from the planner's cache, ensuring that PathKeys with identical characteristics are reused rather than duplicated.

## Definition

```c
PathKey *
make_canonical_pathkey(PlannerInfo *root,
					   EquivalenceClass *eclass, Oid opfamily,
					   int strategy, bool nulls_first)
```
## Detailed Description
This function implements PathKey canonicalization in PostgreSQL's query optimizer. It searches the planner's list of canonical pathkeys () for an existing PathKey that matches the provided parameters. If found, it returns the existing PathKey; otherwise, it creates a new PathKey, adds it to the canonical list, and returns it.

The function ensures that PathKeys are allocated in the main planning context rather than temporary contexts, which is crucial for GEQO (Genetic Query Optimization) scenarios. It also validates that equivalence class merging is complete before creating canonical pathkeys, as the structure must be stable.

The canonicalization process involves chasing up the equivalence class hierarchy to find the top-level (non-merged) equivalence class, ensuring consistency in the canonical representation.

## Parameters / Member Variables
- : PlannerInfo structure containing the query planning context and canonical pathkey list
- : EquivalenceClass that represents a set of expressions considered equivalent for sorting purposes
- : Operator family OID that defines the sorting semantics
- : Strategy number within the operator family (e.g., BTLessStrategyNumber)
- : Boolean indicating whether NULL values should sort before non-NULL values

## Dependencies
- Functions called/Symbols referenced:
  - elog (error logging)
  - lfirst (list iteration)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (memory management)
  - makeNode (node creation)
  - lappend (list append)
- Called from (representative examples):
  - [make_pathkey_from_sortinfo](make_pathkey_from_sortinfo.md)
  - [convert_subquery_pathkeys](../c/convert_subquery_pathkeys.md)
  - [select_outer_pathkeys_for_merge](../s/select_outer_pathkeys_for_merge.md)
  - [make_inner_pathkeys_for_merge](make_inner_pathkeys_for_merge.md)

## Notes and Other Information
- Must not be called until after equivalence class merging is complete ( must be true)
- Uses the main planning context for memory allocation to ensure pathkeys survive temporary context resets
- The function performs equivalence class chasing to handle merged equivalence classes
- Critical for query optimization performance as it prevents duplicate PathKey creation
- Located in src/backend/optimizer/path/pathkeys.c:55-105