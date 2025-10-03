# table_index_fetch_reset

## Location
[src/include/access/tableam.h:1203-1211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1203-L1211)

## Overview
Resets an index fetch operation by releasing cross index fetch resources held in the IndexFetchTableData structure.

## Definition

```c
static inline void
table_index_fetch_reset(struct IndexFetchTableData *scan)
```
## Detailed Description
This function is part of PostgreSQL's table access method (tableam) interface that provides a standardized way to reset index fetch operations. When called, it invokes the table access method's specific  function pointer to clean up any resources that were allocated during index scanning operations. This is typically used to prepare for a new index scan or to clean up after completing one.

The function serves as a thin wrapper around the table access method's implementation, allowing different storage engines to handle resource cleanup in their own specific way while maintaining a consistent interface.

## Parameters / Member Variables
- `*scan`: Pointer to IndexFetchTableData structure containing the index fetch state and associated table relation information
## Dependencies
- Functions called/Symbols referenced:
  - [IndexFetchTableData](../I/IndexFetchTableData.md) (structure type)
  - rd_tableam->index_fetch_reset (table access method function pointer)
- Called from (representative examples):
  - [index_rescan](../i/index_rescan.md) (src/backend/access/index/indexam.c:364)
  - [index_restrpos](../i/index_restrpos.md) (src/backend/access/index/indexam.c:441)
  - [index_parallelrescan](../i/index_parallelrescan.md) (src/backend/access/index/indexam.c:528)
  - [index_getnext_tid](../i/index_getnext_tid.md) (src/backend/access/index/indexam.c:601)

## Notes and Other Information
- This is an inline function defined in the tableam.h header file
- Part of the table access method abstraction layer introduced to support pluggable storage engines
- The actual implementation is delegated to the specific table access method via a function pointer
- Used primarily during index operations to manage resource lifecycle

## Simplified Source

```c
static inline void
table_index_fetch_reset(struct IndexFetchTableData *scan)
{
    // Delegate to table access method's reset implementation
    scan->rel->rd_tableam->index_fetch_reset(scan);
}
```