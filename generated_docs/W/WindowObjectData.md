# WindowObjectData

## Location
src/backend/executor/nodeWindowAgg.c: 62 - 72

## Overview
WindowObjectData is a structure that serves as the context object passed to window functions during execution, containing all necessary state and position information for window function API calls.

## Definition


## Detailed Description
WindowObjectData is the central data structure used in PostgreSQL's window function implementation. It acts as a bridge between the window aggregation executor node and individual window functions, being passed as the context (fcinfo->context) to all window function API calls. This structure encapsulates the state needed for window functions to access rows within their window frame, manage memory, and coordinate with the parent WindowAggState executor node.

The structure maintains pointers into the tuplestore that holds the partition's rows, allowing window functions to efficiently navigate through their window frames. It also manages local memory allocation and maintains references to argument expression states.

## Parameters / Member Variables
- : Standard PostgreSQL node tag for type identification
- : Pointer to the parent WindowAggState executor node that manages overall window aggregation
- : List of ExprState trees representing the evaluated arguments for the window function
- : Memory chunk allocated via WinGetPartitionLocalMemory for function-local storage
- : Tuplestore mark pointer specific to this window function, used for frame positioning
- : Tuplestore read pointer specific to this window function for accessing rows
- : The logical row number that the markptr is currently positioned on
- : The logical row number that the readptr is currently positioned on

## Dependencies
- Functions called/Symbols referenced:
  - WindowAggState
- Called from (representative examples):
  - ExecInitWindowAgg
  - WindowObject (typedef)
  - WindowObjectIsValid

## Notes and Other Information
- This structure is fundamental to PostgreSQL's window function architecture, serving as the primary interface between the executor and window functions
- The mark and read pointers allow efficient random access within the window frame without requiring full rescans
- Memory management is handled through the localmem field, which provides function-local storage that persists across calls within the same partition
- The structure is designed to be lightweight while providing complete access to the windowing context