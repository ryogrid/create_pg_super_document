# CustomScanState

## Location
src/include/nodes/execnodes.h: 2064 - 2073

## Overview
CustomScanState is the execution state node for custom scan implementations in PostgreSQL. It provides an extensible framework allowing extensions and third-party developers to implement custom scan methods with full integration into the PostgreSQL executor.

## Definition
```c
typedef struct CustomScanState
{
    ScanState   ss;
    uint32      flags;          /* mask of CUSTOMPATH_* flags, see
                                 * nodes/extensible.h */
    List       *custom_ps;      /* list of child PlanState nodes, if any */
    Size        pscan_len;      /* size of parallel coordination information */
    const struct CustomExecMethods *methods;
    const struct TupleTableSlotOps *slotOps;
} CustomScanState;
```

## Detailed Description
CustomScanState serves as the foundation for implementing custom scan methods in PostgreSQL through the extensible nodes framework. It allows extensions to define their own scan algorithms while maintaining full compatibility with the PostgreSQL executor infrastructure. The structure provides hooks for custom execution methods, supports parallel query execution, handles child plan states for complex custom operators, and manages tuple slot operations for efficient data handling.

## Parameters / Member Variables
- `ss`: Base ScanState structure containing common scan node fields and NodeTag
- `flags`: Bitmask of CUSTOMPATH_* flags from nodes/extensible.h controlling custom scan behavior and capabilities
- `custom_ps`: List of child PlanState nodes for custom scans that may have sub-plans or complex execution trees
- `pscan_len`: Size of parallel coordination information required for parallel execution of custom scans
- `methods`: Pointer to CustomExecMethods structure containing function pointers for custom scan implementation
- `slotOps`: Pointer to TupleTableSlotOps defining custom tuple slot operations for optimized data handling

## Dependencies
- Functions called/Symbols referenced:
  - ScanState
  - List
  - CustomExecMethods
  - TupleTableSlotOps
- Called from (representative examples):
  - ExecCustomScan
  - ExecInitCustomScan
  - ExecEndCustomScan
  - ExecReScanCustomScan
  - ExecCustomMarkPos
  - ExecCustomRestrPos
  - ExecCustomScanEstimate
  - ExecCustomScanInitializeDSM

## Notes and Other Information
- Core component of PostgreSQL extensible nodes framework for custom scan implementations
- Enables third-party extensions to implement specialized scan algorithms (e.g., GPU acceleration, specialized indexes)
- Supports parallel query execution through pscan_len and parallel coordination mechanisms
- The methods field provides complete control over scan execution through CustomExecMethods function pointers
- Custom tuple slot operations allow for optimized memory management and data representation
- Commonly used by database extensions like postgres_fdw, pg_strom, and other specialized scan providers