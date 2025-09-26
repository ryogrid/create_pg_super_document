# WindowObjectData

## Location
[src/backend/executor/nodeWindowAgg.c:62-72](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L62-L72)

## Overview
WindowObjectData is a structure that serves as the context object passed to window functions during execution, containing all necessary state and position information for window function API calls.

## Definition

```c
typedef struct WindowObjectData
{
	NodeTag		type;
	WindowAggState *winstate;	/* parent WindowAggState */
	List	   *argstates;		/* ExprState trees for fn's arguments */
	void	   *localmem;		/* WinGetPartitionLocalMemory's chunk */
	int			markptr;		/* tuplestore mark pointer for this fn */
	int			readptr;		/* tuplestore read pointer for this fn */
	int64		markpos;		/* row that markptr is positioned on */
	int64		seekpos;		/* row that readptr is positioned on */
} WindowObjectData;
```
## Detailed Description
WindowObjectData is the central data structure used in PostgreSQL's window function implementation. It acts as a bridge between the window aggregation executor node and individual window functions, being passed as the context (fcinfo->context) to all window function API calls. This structure encapsulates the state needed for window functions to access rows within their window frame, manage memory, and coordinate with the parent WindowAggState executor node.

The structure maintains pointers into the tuplestore that holds the partition's rows, allowing window functions to efficiently navigate through their window frames. It also manages local memory allocation and maintains references to argument expression states.

## Parameters / Member Variables
- `type`: Standard PostgreSQL node tag for type identification
- `*winstate`: Pointer to the parent WindowAggState executor node that manages overall window aggregation
- `*argstates`: List of ExprState trees representing the evaluated arguments for the window function
- `*localmem`: Memory chunk allocated via WinGetPartitionLocalMemory for function-local storage
- `markptr`: Tuplestore mark pointer specific to this window function, used for frame positioning
- `readptr`: Tuplestore read pointer specific to this window function for accessing rows
- `markpos`: The logical row number that the markptr is currently positioned on
- `seekpos`: The logical row number that the readptr is currently positioned on
## Dependencies
- Functions called/Symbols referenced:
  - [WindowAggState](WindowAggState.md)
- Called from (representative examples):
  - [ExecInitWindowAgg](../E/ExecInitWindowAgg.md)
  - [WindowObject](WindowObject.md) (typedef)
  - WindowObjectIsValid

## Notes and Other Information
- This structure is fundamental to PostgreSQL's window function architecture, serving as the primary interface between the executor and window functions
- The mark and read pointers allow efficient random access within the window frame without requiring full rescans
- Memory management is handled through the localmem field, which provides function-local storage that persists across calls within the same partition
- The structure is designed to be lightweight while providing complete access to the windowing context