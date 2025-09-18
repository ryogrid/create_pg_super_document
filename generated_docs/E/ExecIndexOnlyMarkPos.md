# ExecIndexOnlyMarkPos

## Location
[src/backend/executor/nodeIndexonlyscan.c:433-469](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIndexonlyscan.c#L433-L469)

## Overview
ExecIndexOnlyMarkPos marks the current position within an index-only scan, enabling the scan to later restore to this marked position, with special handling for EvalPlanQual (EPQ) recheck scenarios.

## Definition
```c
void ExecIndexOnlyMarkPos(IndexOnlyScanState *node)
```

## Detailed Description
This function marks the current position in an index-only scan to enable position restoration later via ExecIndexOnlyRestrPos. The function includes sophisticated logic to handle EvalPlanQual (EPQ) scenarios, which occur during concurrent transaction processing when tuples need to be rechecked for visibility.

When operating within an EPQ recheck context, the function must determine whether to actually mark the index position or skip the operation entirely. If a test tuple exists for the current relation (indicated by populated relsubs_slot or relsubs_rowmark arrays), the function avoids accessing the index and relies on the assumption that the relsubs_done flag is already set, meaning the EPQ processing has progressed beyond the initial scan state.

For normal (non-EPQ) operations, the function simply delegates to the lower-level index_markpos function to mark the current index scan position.

## Parameters / Member Variables
- `node`: Pointer to the IndexOnlyScanState containing the scan state and index scan descriptor to mark

## Dependencies
- Functions called/Symbols referenced:
  - [index_markpos](../i/index_markpos.md)
- Types used:
  - [IndexOnlyScanState](../I/IndexOnlyScanState.md)
  - [EState](EState.md)
  - [EPQState](EPQState.md)
  - Scan
- Called from (representative examples):
  - [ExecMarkPos](ExecMarkPos.md)

## Notes and Other Information
- The function assumes that at least one tuple has been read before marking, ensuring ioss_ScanDesc is not NULL
- EPQ (EvalPlanQual) handling is crucial for maintaining consistency during concurrent transactions
- The function contains assertions to validate EPQ state consistency
- Position marking is essential for implementing cursor-like behavior in SQL queries
- The marked position can be restored using ExecIndexOnlyRestrPos