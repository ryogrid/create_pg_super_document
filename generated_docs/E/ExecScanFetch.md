# ExecScanFetch

## Location
src/backend/executor/execScan.c: 34 - 155

## Overview
ExecScanFetch is a core function in PostgreSQL's executor that fetches the next potential tuple from a scan operation, handling special cases for EvalPlanQual (EPQ) rechecks during concurrent updates.

## Definition


## Detailed Description
ExecScanFetch serves as an intermediary layer between high-level scan execution and access method-specific tuple retrieval. Its primary responsibility is to determine whether to return a regular tuple from the access method or handle special EPQ (EvalPlanQual) recheck scenarios that occur during concurrent transaction processing.

The function first checks for interrupts, then examines if an EPQ recheck is active. During EPQ rechecks, it handles three scenarios: ForeignScan/CustomScan with pushed-down joins, replacement tuples provided by EPQ caller, and fetching tuples using non-locking rowmarks. If no EPQ processing is needed, it delegates to the access method's tuple retrieval function.

## Parameters / Member Variables
- : The ScanState containing execution state information for the scan operation
- : Function pointer to the access method's next-tuple routine (e.g., table scan, index scan)
- : Function pointer to recheck access-method-specific conditions during EPQ

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS
  - ExecClearTuple
  - TupIsNull
  - [EvalPlanQualFetchRowMark](EvalPlanQualFetchRowMark.md)
- Data structures used:
  - [ScanState](../S/ScanState.md)
  - [EPQState](EPQState.md)
  - Scan
  - TupleTableSlot
- Called from:
  - [ExecScan](ExecScan.md) (main caller that orchestrates scan execution)

## Notes and Other Information
- This function is declared as  for performance optimization
- EPQ (EvalPlanQual) handling is crucial for PostgreSQL's MVCC implementation during concurrent updates
- The function handles three distinct EPQ scenarios: pushed-down joins in foreign/custom scans, pre-provided replacement tuples, and rowmark-based tuple fetching
- scanrelid of 0 indicates a ForeignScan or CustomScan with pushed-down operations
- The function maintains proper slot management by clearing tuples that don't meet recheck conditions