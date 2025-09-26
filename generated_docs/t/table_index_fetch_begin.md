# table_index_fetch_begin

## Location
[src/include/access/tableam.h:1193-1202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1193-L1202)

## Overview
Prepares to fetch tuples from a relation as needed for index scan operations, returning an IndexFetchTableData structure for subsequent tuple fetching.

## Definition
```c
static inline IndexFetchTableData *
table_index_fetch_begin(Relation rel)
```

## Detailed Description
This function initializes the necessary data structures and state for fetching tuples from a table during an index scan. It serves as the preparation step before actual tuple retrieval via table_index_fetch_tuple(). The function delegates to the table access method's index_fetch_begin implementation, which sets up the IndexFetchTableData structure containing the context needed for efficient tuple fetching during index scans.

The returned IndexFetchTableData structure maintains state and provides context for subsequent tuple fetch operations, allowing the table access method to optimize tuple retrieval based on the specific storage engine's characteristics.

## Parameters / Member Variables
- `rel`: The relation (table) from which tuples will be fetched during the index scan

## Dependencies
- Functions called/Symbols referenced:
  - [IndexFetchTableData](../I/IndexFetchTableData.md) (return type)
  - rel->rd_tableam->index_fetch_begin (table access method function)
- Called from (representative examples):
  - [index_beginscan](../i/index_beginscan.md)
  - [index_beginscan_parallel](../i/index_beginscan_parallel.md)
  - [table_index_fetch_tuple_check](table_index_fetch_tuple_check.md)
  - [unique_key_recheck](../u/unique_key_recheck.md)

## Notes and Other Information
- This is an inline function defined in the table access method header
- Returns an IndexFetchTableData pointer that must be used with table_index_fetch_tuple()
- Part of the index scan infrastructure in PostgreSQL's table access method API
- The returned structure should be freed with a corresponding table_index_fetch_end() call
- Used extensively in index scanning operations across various parts of the system
- Provides abstraction layer allowing different storage engines to implement their own tuple fetching strategies
- Essential for index-based query execution patterns