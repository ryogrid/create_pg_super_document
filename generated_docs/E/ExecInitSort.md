# ExecInitSort

## Location
[src/backend/executor/nodeSort.c:221-300](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSort.c#L221-L300)

## Overview
Initializes the runtime state for a Sort plan node, creating the SortState structure and setting up execution parameters for tuple sorting operations.

## Definition

```c
structure
	 */
	sortstate = makeNode(SortState);
```
## Detailed Description
ExecInitSort creates and initializes the runtime state information for a Sort node produced by the planner. The function performs several key initialization tasks:

1. **State Structure Creation**: Allocates and initializes a SortState structure with basic plan state information
2. **Execution Strategy Determination**: Analyzes execution flags to determine if random access is needed (for backward scans, mark/restore operations, or rewind capability)
3. **Child Node Initialization**: Recursively initializes the outer subtree while shielding it from complex access pattern requirements
4. **Slot Management**: Sets up scan slots and result tuple slots with appropriate tuple table slot operations
5. **Sort Strategy Selection**: Determines whether to use datum sort (for single-column sorts) or tuple sort (for multi-column sorts) based on the outer plan's tuple descriptor

The function optimizes performance by selecting the most efficient sorting strategy and properly configuring access patterns based on the execution requirements.

## Parameters / Member Variables
- : The Sort plan node containing sort specifications (columns, operators, collations, etc.)
- : The execution state containing global execution context and parameters
- : Execution flags indicating required capabilities (EXEC_FLAG_REWIND, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK)

## Dependencies
- Functions called/Symbols referenced:
  - : Create new SortState node
  - : Set as the execution function for this node
  - : Initialize the outer child plan node
  - : Create scan slot from outer plan
  - : Initialize result tuple slot with minimal tuple operations
  - : Get result tuple descriptor from outer plan
  - /: Access outer plan node and state
- Called from (representative examples):
  - : During executor initialization phase

## Notes and Other Information
- [Sort](../S/Sort.md) nodes do not initialize ExprContexts since they never call ExecQual or ExecProject
- The function shields child nodes from supporting REWIND, BACKWARD, or MARK/RESTORE operations
- Uses TTSOpsVirtual for scan slots and TTSOpsMinimalTuple for result slots for memory efficiency
- Automatically determines datum vs tuple sort strategy based on the number of output columns (natts == 1 triggers datum sort)
- Sets ps_ProjInfo to NULL since Sort nodes perform no projection operations