# ExecSupportsBackwardScan

## Location
src/backend/executor/execAmi.c: 510 - 601

## Overview
ExecSupportsBackwardScan determines whether a complete plan tree supports backward scanning, which is necessary for implementing scrollable cursors and certain query execution patterns.

## Definition


## Detailed Description
ExecSupportsBackwardScan analyzes a complete plan tree to determine if it can support backward scanning operations. This capability is essential for implementing scrollable cursors, where users can move both forward and backward through query results. Unlike mark/restore operations which only need to return to specific marked positions, backward scanning requires the ability to move in reverse through the entire result set.

The function performs a comprehensive analysis of the plan tree, checking not only the capabilities of individual node types but also considering parallel execution constraints and the composition of complex plan structures.

**Key Analysis Areas:**

**Parallel Execution Constraints:**
The function immediately returns false for parallel-aware nodes because parallel execution distributes tuples across multiple workers, making it impossible to maintain the global ordering required for backward scanning.

**Node-Specific Support:**
- **Always Supported**: SeqScan, TidScan, TidRangeScan, FunctionScan, ValuesScan, CteScan, Material, Sort - these nodes maintain complete data sets or can easily reverse their scanning direction
- **Index-Dependent**: IndexScan and IndexOnlyScan support depends on the specific index type capabilities (checked via IndexSupportsBackwardScan)
- **Never Supported**: SampleScan (complexity of sample methods), Gather (parallel execution), IncrementalSort (only keeps single group in memory)
- **Conditionally Supported**: CustomScan nodes must explicitly set CUSTOMPATH_SUPPORT_BACKWARD_SCAN flag

**Recursive Analysis:**
For nodes that pass data through from children (Result, Append, SubqueryScan, LockRows, Limit), the function recursively analyzes child plans to ensure the entire tree supports backward scanning.

**Special Cases:**
- Append nodes with asynchronous subplans (nasyncplans > 0) cannot support backward scanning due to tuple interleaving
- All subplans in an Append must support backward scanning for the whole node to support it

## Parameters / Member Variables
- : Pointer to the Plan node representing the root of the plan tree to analyze. The function recursively examines the entire tree structure to determine backward scan support.

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (node type identification)
  - outerPlan (access outer child plan)
  - [ExecSupportsBackwardScan](ExecSupportsBackwardScan.md) (recursive calls)
  - [IndexSupportsBackwardScan](../I/IndexSupportsBackwardScan.md) (index capability checking)
  - lfirst (list iteration)
- Called from (representative examples):
  - [PerformCursorOpen](../P/PerformCursorOpen.md) (cursor creation)
  - [SPI_cursor_open_internal](../S/SPI_cursor_open_internal.md) (SPI cursor operations)  
  - [standard_planner](../s/standard_planner.md) (query planning)

## Notes and Other Information
- This function requires a complete plan tree because backward scan support often depends on the capabilities of child nodes
- Parallel-aware nodes are automatically excluded due to the complexity of maintaining global ordering across workers
- The function is used during cursor creation to determine if SCROLL capability should be offered to users
- Unlike mark/restore which only needs to return to specific positions, backward scanning requires maintaining the ability to reverse direction at any point
- Some node types like IncrementalSort are excluded because they optimize memory usage by keeping only partial data, making full backward scanning impossible
- Custom scan providers must explicitly declare backward scan support through the CUSTOMPATH_SUPPORT_BACKWARD_SCAN flag
- The function's conservative approach returns false for any unrecognized plan types, ensuring system stability