# CustomScanMethods

## Location
src/include/nodes/extensible.h: 112 - 118

## Overview
CustomScanMethods defines the callback interface for custom scan implementations in PostgreSQL, enabling extensions to provide specialized scan execution logic during query execution.

## Definition
```c
typedef struct CustomScanMethods
{
    const char *CustomName;

    /* Create execution state (CustomScanState) from a CustomScan plan node */
    Node       *(*CreateCustomScanState) (CustomScan *cscan);
} CustomScanMethods;
```

## Detailed Description
CustomScanMethods provides a minimal interface for extensions to implement custom scan operations within PostgreSQL's executor. The primary purpose is to bridge the gap between the planning phase (represented by CustomScan plan nodes) and the execution phase (represented by CustomScanState executor nodes). Extensions register these methods to enable the executor to create appropriate execution state for their custom scan operations.

This structure focuses on the transition from plan to execution state, with the understanding that the real scan logic will be implemented in the custom executor methods (CustomExecMethods).

## Parameters / Member Variables
- `CustomName`: String identifier that uniquely identifies this custom scan method implementation
- `CreateCustomScanState`: Function pointer that creates a CustomScanState executor node from a CustomScan plan node, returning a Node pointer that should actually point to a CustomScanState structure

## Dependencies
- Functions called/Symbols referenced:
  - CustomScan (plan node structure that uses these methods)
  - [Node](../N/Node.md) (return type for state creation)
- Called from (representative examples):
  - [RegisterCustomScanMethods](../R/RegisterCustomScanMethods.md) (registration function)
  - [GetCustomScanMethods](../G/GetCustomScanMethods.md) (lookup function)
  - CustomScan (plan node references these methods)
  - Executor initialization code

## Notes and Other Information
- This interface is simpler than other custom method structures, focusing only on state creation
- The CreateCustomScanState function must return a CustomScanState node (cast to Node*)
- Extensions typically register these methods during module initialization
- The methods field in CustomScan must point to a static table of callback functions (not copied)
- Custom scan methods work in conjunction with CustomExecMethods to provide complete scan functionality
- The created CustomScanState will contain references to CustomExecMethods for actual execution logic
- This design separates plan-time information (CustomScan) from execution-time state (CustomScanState)