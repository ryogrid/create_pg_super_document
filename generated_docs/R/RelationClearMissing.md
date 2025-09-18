# RelationClearMissing

## Location
src/backend/catalog/heap.c: 1947 - 2012

## Overview
RelationClearMissing clears the missing value information (atthasmissing and attmissingval) for all attributes of a relation, used when the table is rewritten and no longer needs missing value defaults.

## Definition


## Detailed Description
This function removes missing value information from all attributes in a relation by setting atthasmissing to false and attmissingval to null in pg_attribute. It is safely used when a table is completely rewritten (such as by VACUUM FULL or CLUSTER) where all rows are guaranteed to have the full complement of attributes, making missing value defaults unnecessary. The function iterates through all non-system attributes, finds those with atthasmissing set to true, and updates their pg_attribute entries to clear the missing value information. This optimization reduces storage overhead and eliminates unnecessary missing value processing.

## Parameters / Member Variables
- `rel`: Relation object for which to clear missing value information (caller must hold AccessExclusive lock)

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetNumberOfAttributes
  - SearchSysCache2
  - heap_modify_tuple
  - CatalogTupleUpdate
  - heap_freetuple
- Called from (representative examples):
  - finish_heap_swap
  - ATExecSetExpression
  - ATExecAlterColumnType

## Notes and Other Information
- Requires AccessExclusive lock on the relation (must be held by caller)
- Only processes attributes where atthasmissing is currently true
- Sets atthasmissing to false and attmissingval to null for applicable attributes
- Processes all non-system attributes including dropped columns
- Triggers automatic relcache rebuild when pg_attribute rows are updated
- Commonly used after table rewrites (VACUUM FULL, CLUSTER, ALTER COLUMN TYPE)
- Safe to call when all table rows have full attribute complement
- Improves performance by eliminating unnecessary missing value processing
- Uses heap_modify_tuple to update pg_attribute entries efficiently