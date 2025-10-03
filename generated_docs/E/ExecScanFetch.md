# ExecScanFetch

## Location
[src/backend/executor/execScan.c:34-155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execScan.c#L34-L155)

## Overview
ExecScanFetch is a core function in PostgreSQL's executor that fetches the next potential tuple from a scan operation, handling special cases for EvalPlanQual (EPQ) rechecks during concurrent updates.

## Definition

```c
static inline TupleTableSlot *
ExecScanFetch(ScanState *node,
			  ExecScanAccessMtd accessMtd,
			  ExecScanRecheckMtd recheckMtd)
```
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
  - [ExecClearTuple](ExecClearTuple.md)
  - TupIsNull
  - [EvalPlanQualFetchRowMark](EvalPlanQualFetchRowMark.md)
- Data structures used:
  - [ScanState](../S/ScanState.md)
  - [EPQState](EPQState.md)
  - [Scan](../S/Scan.md)
  - [TupleTableSlot](../T/TupleTableSlot.md)
- Called from:
  - [ExecScan](ExecScan.md) (main caller that orchestrates scan execution)

## Notes and Other Information
- This function is declared as  for performance optimization
- EPQ (EvalPlanQual) handling is crucial for PostgreSQL's MVCC implementation during concurrent updates
- The function handles three distinct EPQ scenarios: pushed-down joins in foreign/custom scans, pre-provided replacement tuples, and rowmark-based tuple fetching
- scanrelid of 0 indicates a ForeignScan or CustomScan with pushed-down operations
- The function maintains proper slot management by clearing tuples that don't meet recheck conditions

## Simplified Source

```c
static inline TupleTableSlot *
ExecScanFetch(ScanState *node,
              ExecScanAccessMtd accessMtd,
              ExecScanRecheckMtd recheckMtd)
{
    EState *estate = node->ps.state;

    CHECK_FOR_INTERRUPTS();

    // Handle EPQ (EvalPlanQual) recheck if active
    if (estate->es_epq_active != NULL) {
        EPQState *epqstate = estate->es_epq_active;
        Index scanrelid = ((Scan *) node->ps.plan)->scanrelid;

        if (scanrelid == 0) {
            // ForeignScan/CustomScan with pushed-down join
            TupleTableSlot *slot = node->ss_ScanTupleSlot;

            if (!(*recheckMtd) (node, slot))
                ExecClearTuple(slot);  // Failed recheck
            return slot;
        }
        else if (epqstate->relsubs_done[scanrelid - 1]) {
            // Already processed EPQ tuple for this relation
            TupleTableSlot *slot = node->ss_ScanTupleSlot;
            return ExecClearTuple(slot);
        }
        else if (epqstate->relsubs_slot[scanrelid - 1] != NULL) {
            // Use replacement tuple provided by EPQ caller
            TupleTableSlot *slot = epqstate->relsubs_slot[scanrelid - 1];

            // Mark as done to avoid returning again
            epqstate->relsubs_done[scanrelid - 1] = true;

            if (TupIsNull(slot))
                return NULL;

            // Check if it meets access-method conditions
            if (!(*recheckMtd) (node, slot))
                return ExecClearTuple(slot);
            return slot;
        }
        else if (epqstate->relsubs_rowmark[scanrelid - 1] != NULL) {
            // Fetch replacement tuple using non-locking rowmark
            TupleTableSlot *slot = node->ss_ScanTupleSlot;

            epqstate->relsubs_done[scanrelid - 1] = true;

            if (!EvalPlanQualFetchRowMark(epqstate, scanrelid, slot))
                return NULL;

            if (TupIsNull(slot))
                return NULL;

            // Check if it meets access-method conditions
            if (!(*recheckMtd) (node, slot))
                return ExecClearTuple(slot);
            return slot;
        }
    }

    // Normal case: get next tuple from access method
    return (*accessMtd) (node);
}
```