# TuplesortMethod

## Location
[src/include/utils/tuplesort.h:82-83](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/tuplesort.h#L82-L83)

## Overview
TuplesortMethod is an enumeration type that represents different sorting algorithms used by PostgreSQL's tuple sorting subsystem to track which specific sorting method was employed during a sort operation.

## Definition

```c
typedef enum
{
	SORT_SPACE_TYPE_DISK,
	SORT_SPACE_TYPE_MEMORY,
} TuplesortSpaceType;
```
## Detailed Description
TuplesortMethod is a bitmask enumeration that categorizes the different sorting algorithms available in PostgreSQL's tuplesort module. Each value represents a distinct sorting strategy that PostgreSQL can employ based on the characteristics of the data being sorted, memory availability, and performance requirements.

The enumeration uses bit flags (powers of 2) to allow for efficient combination and checking of multiple sort methods, particularly useful for instrumentation and reporting purposes where multiple methods might be tracked simultaneously.

## Parameters / Member Variables
- : Indicates that the sorting operation is still ongoing and the final method has not yet been determined
- : Uses heap sort algorithm, typically employed for ORDER BY ... LIMIT queries where only the top N results are needed
- : Uses quicksort algorithm for in-memory sorting when the dataset fits entirely in available memory
- : Uses external sorting when the dataset is too large to fit in memory, involving temporary file storage
- : Uses external merge sort algorithm to combine pre-sorted runs from temporary files

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a standalone enumeration type)
- Called from (representative examples):
  - [tuplesort_method_name](../t/tuplesort_method_name.md)
  - [show_incremental_sort_group_info](../s/show_incremental_sort_group_info.md)
  - [TuplesortInstrumentation](TuplesortInstrumentation.md) (as a member variable)

## Notes and Other Information
- The enumeration is defined with  set to 4, representing the number of actual sorting methods (excluding the in-progress state)
- Used extensively in the tuplesort instrumentation system to track and report which sorting algorithms were used during query execution
- The bitmask design allows for efficient bitwise operations when checking or combining multiple sort methods
- Each sorting method is chosen automatically by PostgreSQL based on factors like data size, available memory, and query characteristics
- The tuplesort_method_name() function provides string representations of these enum values for display in EXPLAIN output and logging