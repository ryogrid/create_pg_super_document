# table_index_fetch_begin

## Location
src/include/access/tableam.h: 1193 - 1202

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
  - IndexFetchTableData (return type)
  - rel->rd_tableam->index_fetch_begin (table access method function)
- Called from (representative examples):
  - index_beginscan
  - index_beginscan_parallel
  - table_index_fetch_tuple_check
  - unique_key_recheck

## Notes and Other Information
- This is an inline function defined in the table access method header
- Returns an IndexFetchTableData pointer that must be used with table_index_fetch_tuple()
- Part of the index scan infrastructure in PostgreSQL's table access method API
- The returned structure should be freed with a corresponding table_index_fetch_end() call
- Used extensively in index scanning operations across various parts of the system
- Provides abstraction layer allowing different storage engines to implement their own tuple fetching strategies
- Essential for index-based query execution patterns