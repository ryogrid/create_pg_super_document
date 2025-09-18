# ExecReScanSampleScan

## Location
src/backend/executor/nodeSamplescan.c: 202 - 217

## Overview
Resets a sample scan node to enable re-scanning the relation from the beginning, clearing all state variables that track the scan progress.

## Definition
void ExecReScanSampleScan(SampleScanState *node)

## Detailed Description
ExecReScanSampleScan is responsible for resetting the state of a sample scan operation so that the relation can be scanned again from the start. This function is part of PostgreSQL's execution node infrastructure and is called when a rescan operation is required on a sample scan node. The function resets key state flags that track whether the scan has begun, completed, or has a current block, and resets the tuple counter. It then delegates to the generic scan rescan functionality.

## Parameters / Member Variables
- `node`: Pointer to the SampleScanState containing the scan state information to be reset

## Dependencies
- Functions called/Symbols referenced:
  - ExecScanReScan (to reset the underlying scan state)
  - SampleScanState (the state structure being modified)
- Called from (representative examples):
  - ExecReScan (in execAmi.c:169)
  - NODESAMPLESCAN_H (header declaration in nodeSamplescan.h:21)

## Notes and Other Information
The function specifically resets four key state variables:
- `begun`: Set to false to indicate BeginSampleScan needs to be called again
- `done`: Set to false to indicate the scan is not completed
- `haveblock`: Set to false to indicate no current block is available
- `donetuples`: Reset to 0 to restart the tuple counter

This function is essential for supporting PostgreSQL's ability to restart scans, which is required for certain query execution scenarios like nested loops.