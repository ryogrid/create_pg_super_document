# ExecForeignScanEstimate

## Location
src/backend/executor/nodeForeignscan.c: 356 - 374

## Overview
ExecForeignScanEstimate estimates the size of shared memory required for parallel foreign scan coordination information.

## Definition
void ExecForeignScanEstimate(ForeignScanState *node, ParallelContext *pcxt)

## Detailed Description
This function estimates the amount of dynamic shared memory (DSM) space needed for parallel foreign scan operations. It calls the foreign data wrapper's EstimateDSMForeignScan routine (if provided) to determine the required memory size, then updates the parallel context's shared memory estimator with chunk and key requirements. This is part of PostgreSQL's parallel query execution infrastructure.

## Parameters / Member Variables
- : Pointer to the ForeignScanState containing the execution state for the foreign scan operation
- : Pointer to the ParallelContext structure used for coordinating parallel execution

## Dependencies
- Functions called/Symbols referenced:
  - shm_toc_estimate_chunk
  - shm_toc_estimate_keys
  - FdwRoutine.EstimateDSMForeignScan (if available)
- Called from (representative examples):
  - ExecParallelEstimate

## Notes and Other Information
- Only performs estimation if the foreign data wrapper provides an EstimateDSMForeignScan routine
- The estimated size is stored in node->pscan_len for later use during DSM initialization
- Estimates one shared memory key for the foreign scan coordination data
- Part of PostgreSQL's parallel query execution framework
- Located in src/backend/executor/nodeForeignscan.c:356-374