# BTOptions

## Location
[src/include/access/nbtree.h:1130-1136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L1130-L1136)

## Overview
BTOptions is a structure that holds configuration options for B-tree indexes, including storage parameters like fill factor and deduplication settings.

## Definition

```c
typedef struct BTOptions
{
	int32		varlena_header_;	/* varlena header (do not touch directly!) */
	int			fillfactor;		/* page fill factor in percent (0..100) */
	float8		vacuum_cleanup_index_scale_factor;	/* deprecated */
	bool		deduplicate_items;	/* Try to deduplicate items? */
} BTOptions;
```
## Detailed Description
BTOptions encapsulates the configuration parameters that can be set for B-tree indexes in PostgreSQL. This structure follows the PostgreSQL varlena convention, allowing it to be stored as a variable-length data type. The structure contains settings that affect both storage efficiency and performance characteristics of B-tree indexes, including how densely pages should be packed and whether duplicate items should be deduplicated to save space.

## Parameters / Member Variables
- `varlena_header_`: Standard PostgreSQL varlena header required for variable-length data types (should not be accessed directly)
- `fillfactor`: Percentage value (0-100) controlling how full each index page should be during initial index creation and bulk operations
- `vacuum_cleanup_index_scale_factor`: Deprecated parameter previously used to control vacuum behavior on indexes
- `deduplicate_items`: Boolean flag indicating whether the index should attempt to deduplicate identical key values to reduce storage space
## Dependencies
- Functions called/Symbols referenced:
  - int32 (PostgreSQL type)
  - float8 (PostgreSQL type)
  - [bool](../b/bool.md) (PostgreSQL type)
- Called from (representative examples):
  - [btoptions](../b/btoptions.md) (src/backend/access/nbtree/nbtutils.c:4566-4575)
  - BTGetFillFactor (src/include/access/nbtree.h:1142)
  - BTGetDeduplicateItems (src/include/access/nbtree.h:1150)

## Notes and Other Information
The fillfactor setting is particularly important for write-heavy workloads as it controls the trade-off between storage efficiency and update performance. A lower fillfactor leaves more free space on each page, reducing page splits during inserts but using more storage. The deduplicate_items option can significantly reduce index size for indexes with many duplicate values, especially useful for non-unique indexes. The vacuum_cleanup_index_scale_factor field is deprecated and maintained only for backward compatibility.