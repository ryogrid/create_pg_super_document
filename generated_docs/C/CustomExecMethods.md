# CustomExecMethods

## Location
[src/include/nodes/extensible.h:124-158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/extensible.h#L124-L158)

## Overview
CustomExecMethods defines the comprehensive callback interface for executing custom scan operations in PostgreSQL's executor, providing hooks for all phases of scan execution including initialization, tuple retrieval, cleanup, parallelization, and debugging.

## Definition
```c
typedef struct CustomExecMethods
{
    const char *CustomName;

    /* Required executor methods */
    void        (*BeginCustomScan) (CustomScanState *node,
                                   EState *estate,
                                   int eflags);
    TupleTableSlot *(*ExecCustomScan) (CustomScanState *node);
    void        (*EndCustomScan) (CustomScanState *node);
    void        (*ReScanCustomScan) (CustomScanState *node);

    /* Optional methods: needed if mark/restore is supported */
    void        (*MarkPosCustomScan) (CustomScanState *node);
    void        (*RestrPosCustomScan) (CustomScanState *node);

    /* Optional methods: needed if parallel execution is supported */
    Size        (*EstimateDSMCustomScan) (CustomScanState *node,
                                         ParallelContext *pcxt);
    void        (*InitializeDSMCustomScan) (CustomScanState *node,
                                           ParallelContext *pcxt,
                                           void *coordinate);
    void        (*ReInitializeDSMCustomScan) (CustomScanState *node,
                                             ParallelContext *pcxt,
                                             void *coordinate);
    void        (*InitializeWorkerCustomScan) (CustomScanState *node,
                                              shm_toc *toc,
                                              void *coordinate);
    void        (*ShutdownCustomScan) (CustomScanState *node);

    /* Optional: print additional information in EXPLAIN */
    void        (*ExplainCustomScan) (CustomScanState *node,
                                     List *ancestors,
                                     ExplainState *es);
} CustomExecMethods;
```

## Detailed Description
CustomExecMethods provides the most comprehensive interface among the extensible node method structures, enabling extensions to implement full-featured custom scan operations within PostgreSQL's executor. This structure contains callbacks for all aspects of scan execution: lifecycle management (begin/exec/end/rescan), position management (mark/restore), parallel execution support, and query explanation. Extensions must implement the required methods and can optionally implement advanced features like parallel execution and position marking.

The structure supports PostgreSQL's advanced execution features including parallel query execution, positioned scans for joins, and detailed query explanation, making custom scans first-class citizens in the execution engine.

## Parameters / Member Variables
- `CustomName`: String identifier that uniquely identifies this custom execution method implementation
- `BeginCustomScan`: **Required** - Initialize the custom scan execution state with executor state and flags
- `ExecCustomScan`: **Required** - Execute the scan and return the next tuple (or NULL when done)
- `EndCustomScan`: **Required** - Clean up resources and finalize the scan execution
- `ReScanCustomScan`: **Required** - Reset the scan to restart from the beginning
- `MarkPosCustomScan`: **Optional** - Mark the current scan position for later restoration (needed for merge joins)
- `RestrPosCustomScan`: **Optional** - Restore to previously marked scan position
- `EstimateDSMCustomScan`: **Optional** - Estimate shared memory needed for parallel execution coordination
- `InitializeDSMCustomScan`: **Optional** - Initialize shared memory for parallel execution coordination
- `ReInitializeDSMCustomScan`: **Optional** - Reinitialize shared memory for parallel execution
- `InitializeWorkerCustomScan`: **Optional** - Initialize custom scan in a parallel worker process
- `ShutdownCustomScan`: **Optional** - Clean up after parallel execution completion
- `ExplainCustomScan`: **Optional** - Provide additional information for EXPLAIN output

## Dependencies
- Functions called/Symbols referenced:
  - [CustomScanState](CustomScanState.md) (execution state structure)
  - [EState](../E/EState.md) (executor state)
  - TupleTableSlot (tuple storage)
  - [ParallelContext](../P/ParallelContext.md) (parallel execution context)
  - ExplainState (query explanation state)
  - [shm_toc](../s/shm_toc.md) (shared memory table of contents)
  - [List](../L/List.md) (PostgreSQL list structure)
- Called from (representative examples):
  - [ExecCustomScanEstimate](../E/ExecCustomScanEstimate.md) (parallel execution estimation)
  - [ExecCustomScanInitializeDSM](../E/ExecCustomScanInitializeDSM.md) (parallel initialization)
  - [ExecCustomScanInitializeWorker](../E/ExecCustomScanInitializeWorker.md) (worker initialization)
  - [ExecShutdownCustomScan](../E/ExecShutdownCustomScan.md) (parallel shutdown)
  - [CustomScanState](CustomScanState.md) (execution state references these methods)

## Notes and Other Information
- This is the most complex of the custom method interfaces, supporting full executor integration
- The four required methods (Begin/Exec/End/ReScan) must be implemented by all custom scans
- Mark/restore position methods are only needed if the scan supports positioned access for merge joins
- Parallel execution methods enable custom scans to participate in PostgreSQL's parallel query execution
- The ExplainCustomScan method allows custom scans to provide detailed information in query plans
- Custom scans with these methods can achieve performance equivalent to built-in scan types
- Extensions typically register these methods and reference them in CustomScanState structures
- The design enables custom scans to support advanced PostgreSQL features like parallel execution and complex join algorithms