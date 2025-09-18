# GetHeapamTableAmRoutine

## Location
[src/backend/access/heap/heapam_handler.c:2653-2658](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L2653-L2658)

## Overview
GetHeapamTableAmRoutine returns a pointer to the heap access method's complete TableAmRoutine structure, providing the interface for all heap table operations.

## Definition
```c
const TableAmRoutine *
GetHeapamTableAmRoutine(void)
```

## Detailed Description
This function serves as the primary entry point to access the heap access method's complete function table (heapam_methods). It returns a pointer to a static TableAmRoutine structure that contains function pointers for all heap table operations including scanning, tuple manipulation, indexing, vacuum, and analysis operations.

The returned TableAmRoutine structure provides a comprehensive interface that implements PostgreSQL's table access method API for heap tables. This includes operations for:
- Table scanning (sequential, parallel, TID range, bitmap, sampling)
- Index operations (fetch, build, validate, delete)
- Tuple operations (insert, update, delete, lock, fetch)
- Relation operations (vacuum, copy, clustering, size estimation)
- TOAST operations and analysis functions

This function is used by PostgreSQL's table access method infrastructure to obtain the heap-specific implementations of all table operations.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - heapam_methods (static TableAmRoutine structure)
- Called from (representative examples):
  - [heap_getnext](../h/heap_getnext.md)
  - [formrdesc](../f/formrdesc.md)
  - table_scan_sample_next_tuple

## Notes and Other Information
- This is a simple accessor function that returns a reference to the static heapam_methods structure
- The heapam_methods structure contains approximately 40+ function pointers implementing the complete table access method interface
- Used by PostgreSQL's pluggable table access method architecture to provide heap-specific implementations
- The function has no side effects and can be called safely from any context
- Part of the heap access method's public interface for integration with the table access method framework