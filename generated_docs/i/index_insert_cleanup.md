# index_insert_cleanup

## Location
[src/backend/access/index/indexam.c:241-255](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/indexam.c#L241-L255)

## Overview
The  function performs cleanup operations after all index insertions are completed during batch index operations.

## Definition

```c
void
index_insert_cleanup(Relation indexRelation,
					 IndexInfo *indexInfo)
```
## Detailed Description
This function serves as a wrapper that calls the access method-specific cleanup routine for index insertions. It checks if the index access method provides a cleanup function () and calls it if available. This allows different index types (B-tree, hash, GiST, etc.) to perform their own specific cleanup operations after batch insertions are complete. The function includes relation validation checks to ensure the index relation is in a valid state before proceeding.

## Parameters / Member Variables
- : The index relation on which cleanup operations should be performed
- : Structure containing metadata about the index, including information needed for cleanup operations

## Dependencies
- Functions called/Symbols referenced:
  - RELATION_CHECKS (macro for relation validation)
  - IndexInfo (structure type)
  - [IndexScanDesc](../I/IndexScanDesc.md) (structure type)
- Called from (representative examples):
  - [validate_index](../v/validate_index.md) (src/backend/catalog/index.c:3401)
  - [unique_key_recheck](../u/unique_key_recheck.md) (src/backend/commands/constraint.c:179)
  - [ExecCloseIndices](../E/ExecCloseIndices.md) (src/backend/executor/execIndexing.c:248)

## Notes and Other Information
- This function is part of PostgreSQL's index access method abstraction layer
- The actual cleanup work is delegated to the specific access method implementation
- Not all index types may require cleanup operations, so the aminsertcleanup function pointer may be NULL
- Located in src/backend/access/index/indexam.c:241-255