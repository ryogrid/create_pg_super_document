# pathkey_is_redundant

## Location
[src/backend/optimizer/path/pathkeys.c:158-196](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L158-L196)

## Overview
Determines whether a PathKey is redundant with respect to an existing list of PathKeys, helping optimize query plans by eliminating unnecessary sort operations.

## Definition

```c
static bool
pathkey_is_redundant(PathKey *new_pathkey, List *pathkeys)
```
## Detailed Description
This function implements sophisticated redundancy detection for PathKeys in PostgreSQL's query optimizer. It identifies two key cases where a PathKey can be considered redundant:

1. **Constant Equivalence Class**: If the new PathKey's equivalence class contains a constant (detected via EC_MUST_BE_REDUNDANT macro), the PathKey provides no ordering information since all values are effectively the same. For example, in , sorting by  is redundant.

2. **Duplicate Equivalence Class**: If the new PathKey's equivalence class is identical to any existing PathKey in the list, it's redundant regardless of sort direction or operator family. Examples include  or .

The function relies on the canonical nature of equivalence classes, where pointer comparison is sufficient to determine equality since the equivclass.c machinery ensures only one copy of each EC exists per query.

## Parameters / Member Variables
- : The PathKey being tested for redundancy
- : List of existing PathKeys to check against

## Dependencies
- Functions called/Symbols referenced:
  - EC_MUST_BE_REDUNDANT (macro for detecting constant equivalence classes)
  - lfirst (list iteration)
- Called from (representative examples):
  - [append_pathkeys](../a/append_pathkeys.md)
  - [build_index_pathkeys](../b/build_index_pathkeys.md)
  - [build_partition_pathkeys](../b/build_partition_pathkeys.md)
  - [convert_subquery_pathkeys](../c/convert_subquery_pathkeys.md)
  - [make_pathkeys_for_sortclauses_extended](../m/make_pathkeys_for_sortclauses_extended.md)
  - [select_outer_pathkeys_for_merge](../s/select_outer_pathkeys_for_merge.md)
  - [make_inner_pathkeys_for_merge](../m/make_inner_pathkeys_for_merge.md)

## Notes and Other Information
- Both the input PathKey and list members must be canonical for proper operation
- Uses pointer comparison for equivalence class equality checking due to EC uniqueness guarantees
- Critical for query optimization performance by preventing redundant sort operations
- Handles complex cases like constant propagation and equivalent expressions
- Does not compare operator families or sort directions when checking EC equivalence
- Located in src/backend/optimizer/path/pathkeys.c:158-196