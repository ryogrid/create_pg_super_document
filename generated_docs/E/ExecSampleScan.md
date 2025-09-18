# ExecSampleScan

## Location
src/backend/executor/nodeSamplescan.c: 79 - 92

## Overview
ExecSampleScan is the main execution function for sample scan operations that retrieves the next qualifying tuple from a relation using the specified sampling method.

## Definition


## Detailed Description
ExecSampleScan serves as the primary execution entry point for sample scan nodes in PostgreSQL's executor. It implements the standard scan node interface by delegating to the generic ExecScan function, providing it with sample-specific access method functions. The function first casts the generic PlanState to a SampleScanState to access sample-specific state information, then calls ExecScan with SampleNext as the tuple access method and SampleRecheck as the recheck method. This design follows PostgreSQL's executor pattern where each scan type provides specific access methods while sharing common scanning logic through ExecScan.

## Parameters / Member Variables
- : A pointer to the generic PlanState structure that gets cast to SampleScanState for sample-specific operations

## Dependencies
- Functions called/Symbols referenced:
  - ExecScan
  - SampleNext
  - SampleRecheck
  - SampleScanState (type)
  - castNode (macro)
- Called from (representative examples):
  - ExecInitSampleScan (as ps_ExecProcNode function pointer)

## Notes and Other Information
- This is a static function, accessible only within nodeSamplescan.c
- Follows the standard PostgreSQL executor pattern of delegating to ExecScan with node-specific access methods
- The function is typically assigned to the ps_ExecProcNode function pointer during node initialization
- Returns a TupleTableSlot containing the next sampled tuple, or NULL when the sample is exhausted
- Part of the executor node interface that enables sample scans to be used in query plans like other scan operations