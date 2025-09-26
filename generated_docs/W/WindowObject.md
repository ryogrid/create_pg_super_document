# WindowObject

## Location
[src/include/windowapi.h:37-38](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/windowapi.h#L37-L38)

## Overview
WindowObject is an opaque pointer type that represents the execution context passed to PostgreSQL window functions, providing access to partition data, current position information, and argument evaluation capabilities.

## Definition

```c
typedef struct WindowObjectData *WindowObject;
```
## Detailed Description
WindowObject serves as the primary interface between window functions and the PostgreSQL window aggregation executor. It is an opaque pointer to a WindowObjectData structure that contains all the necessary context for a window function to operate within its partition and frame.

The WindowObject is passed to window functions through the fcinfo->context field and can be retrieved using the PG_WINDOW_OBJECT() macro. Window functions use this object to:
- Determine their current position within the partition
- Access the total number of rows in the partition
- Evaluate their argument expressions at different row positions
- Manage partition-local memory
- Set mark positions for efficient data access

The actual WindowObjectData structure is private to nodeWindowAgg.c and contains pointers to the WindowAggState, expression states for function arguments, tuple store pointers, and position tracking information.

## Parameters / Member Variables
As an opaque pointer type, WindowObject itself has no directly accessible members. The underlying WindowObjectData structure contains:
- : NodeTag for type identification
- : Pointer to parent WindowAggState
- : List of ExprState trees for function arguments
- : Partition-local memory chunk
- : Tuplestore mark pointer for this function
- : Tuplestore read pointer for this function
- : Row position of the mark pointer
- : Row position of the read pointer

## Dependencies
- Functions called/Symbols referenced:
  - [WindowObjectData](WindowObjectData.md) (underlying structure)
  
- Called from (representative examples):
  - [window_row_number](../w/window_row_number.md) (window function implementation)
  - [window_rank](../w/window_rank.md) (window function implementation)
  - [window_dense_rank](../w/window_dense_rank.md) (window function implementation)
  - [window_percent_rank](../w/window_percent_rank.md) (window function implementation)
  - [window_cume_dist](../w/window_cume_dist.md) (window function implementation)
  - [window_ntile](../w/window_ntile.md) (window function implementation)
  - [window_first_value](../w/window_first_value.md) (window function implementation)
  - [window_last_value](../w/window_last_value.md) (window function implementation)
  - [window_nth_value](../w/window_nth_value.md) (window function implementation)
  - [leadlag_common](../l/leadlag_common.md) (helper for LAG/LEAD functions)

## Notes and Other Information
- Window functions must use the V1 calling convention to receive a WindowObject
- The WindowObject validity can be tested using WindowObjectIsValid() macro
- Window functions should not directly access the WindowObjectData structure but use the provided API functions:
  - [WinGetPartitionLocalMemory](WinGetPartitionLocalMemory.md)() - Allocate partition-local memory
  - [WinGetCurrentPosition](WinGetCurrentPosition.md)() - Get current row position
  - [WinGetPartitionRowCount](WinGetPartitionRowCount.md)() - Get total rows in partition
  - [WinSetMarkPosition](WinSetMarkPosition.md)() - Set mark position for efficient access
  - [WinRowsArePeers](WinRowsArePeers.md)() - Check if two rows are peers
  - [WinGetFuncArgInPartition](WinGetFuncArgInPartition.md)() - Evaluate argument at any partition row
  - [WinGetFuncArgInFrame](WinGetFuncArgInFrame.md)() - Evaluate argument at any frame row
  - [WinGetFuncArgCurrent](WinGetFuncArgCurrent.md)() - Evaluate argument at current row
- The WindowObject provides the foundation for PostgreSQL's window function framework and enables efficient implementation of complex analytical functions