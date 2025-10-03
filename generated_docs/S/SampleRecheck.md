# SampleRecheck

## Location
[src/backend/executor/nodeSamplescan.c:60-78](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSamplescan.c#L60-L78)

## Overview
SampleRecheck is a recheck function used during EvalPlanQual processing for sample scans that always returns true since sample scans don't use checkable scan keys.

## Definition

```c
structure
	 */
	scanstate = makeNode(SampleScanState);
```
## Detailed Description
SampleRecheck is an access method routine specifically designed for use during EvalPlanQual (EPQ) processing in PostgreSQL's concurrency control mechanism. During EPQ, the system needs to re-evaluate whether previously found tuples still satisfy the scan conditions after potential concurrent updates. However, for sample scans, this function always returns true because sample scans, like sequential scans, don't pass any checkable keys to the underlying heap scan operation. The sampling logic is based on the sampling method's algorithm rather than tuple-level predicates that could be invalidated by concurrent updates.

## Parameters / Member Variables
- : A pointer to the SampleScanState structure containing the sample scan's state information
- : A TupleTableSlot containing the tuple to be rechecked

## Dependencies
- Functions called/Symbols referenced:
  - [SampleScanState](SampleScanState.md) (type reference)
- Called from (representative examples):
  - [ExecSampleScan](../E/ExecSampleScan.md)

## Notes and Other Information
- This is a static function, only accessible within nodeSamplescan.c
- Always returns true, indicating that no additional checking is needed during EPQ processing
- Part of the standard scan interface that all scan nodes must implement for EvalPlanQual support
- The simplicity reflects that sampling is done at the storage/access level rather than through tuple-level filtering
- Similar to SeqScan's recheck behavior, emphasizing the parallel between sequential and sample scanning

## Simplified Source

```c
static bool
SampleRecheck(SampleScanState *node, TupleTableSlot *slot)
{
    // Sample scans don't use checkable keys, so no recheck needed
    return true;
}
```