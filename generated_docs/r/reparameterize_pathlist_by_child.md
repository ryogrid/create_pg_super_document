# reparameterize_pathlist_by_child

## Location
[src/backend/optimizer/util/pathnode.c:4542-4570](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L4542-L4570)

## Overview
Helper function that reparameterizes a list of paths by a given child relation, used during query planning to translate parameterized paths from parent to child context.

## Definition
static List *reparameterize_pathlist_by_child(PlannerInfo *root, List *pathlist, RelOptInfo *child_rel)

## Detailed Description
This function processes a list of paths that are parameterized by the parent of the given child relation and translates them to be parameterized by the child relation instead. It iterates through each path in the input list and calls reparameterize_path_by_child() for individual path reparameterization.

The function implements an all-or-nothing approach: if any path in the list cannot be reparameterized, the entire operation fails and returns NIL. This ensures consistency across the entire path list. The function is used internally by PostgreSQL's query planner during the creation of execution plans when dealing with partitioned tables or inheritance hierarchies.

## Parameters / Member Variables
- root: PlannerInfo structure containing global information about the current planning context
- pathlist: List of Path structures to be reparameterized (must not be NIL as NIL return indicates failure)
- child_rel: RelOptInfo representing the child relation that will become the new parameterization context

## Dependencies
- Functions called/Symbols referenced:
  - [reparameterize_path_by_child](reparameterize_path_by_child.md) (core reparameterization logic for individual paths)
  - [list_free](../l/list_free.md) (memory cleanup when operation fails)
  - [lappend](../l/lappend.md) (list manipulation for building result)
  - lfirst (list traversal)
- Called from (representative examples):
  - REPARAMETERIZE_CHILD_PATH_LIST macro

## Notes and Other Information
- Returns NIL to indicate failure, so input pathlist should never be NIL to avoid ambiguity
- The function is static (internal to pathnode.c) and serves as a building block for higher-level path reparameterization operations
- Memory management is handled carefully: if any path fails reparameterization, previously allocated results are freed before returning NIL
- This function is part of PostgreSQL's sophisticated query optimization system for handling partitioned tables and inheritance hierarchies efficiently