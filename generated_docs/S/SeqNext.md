# SeqNext

## Location
[src/backend/executor/nodeSeqscan.c:50-88](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSeqscan.c#L50-L88)

## Overview
SeqNext is a static function that serves as the core workhorse for ExecSeqScan, responsible for retrieving the next tuple from a sequential table scan.

## Definition

```c
static TupleTableSlot *
SeqNext(SeqScanState *node)
```
## Detailed Description
SeqNext implements the fundamental tuple retrieval logic for sequential scans in PostgreSQL. It manages the table scan descriptor lifecycle, handles both parallel and non-parallel scan scenarios, and retrieves tuples from the underlying storage engine. The function first checks if a scan descriptor exists, and if not (which occurs for non-parallel scans or serial execution of planned parallel scans), it initializes one using table_beginscan. It then calls table_scan_getnextslot to fetch the next tuple in the specified scan direction.

## Parameters / Member Variables
- `node`: SeqScanState pointer containing the scan state information, including the current relation, scan descriptor, and tuple slot

## Dependencies
- Functions called/Symbols referenced:
  - [table_beginscan](../t/table_beginscan.md)
  - [table_scan_getnextslot](../t/table_scan_getnextslot.md)
  - [SeqScanState](SeqScanState.md)
  - [TableScanDesc](../T/TableScanDesc.md)
  - ScanDirection
- Called from (representative examples):
  - [ExecSeqScan](../E/ExecSeqScan.md)

## Notes and Other Information
- This is a static function, only accessible within nodeSeqscan.c
- Handles the distinction between parallel and non-parallel scan execution
- Returns NULL when no more tuples are available
- The function manages scan descriptor initialization lazily for non-parallel scans

## Simplified Source

```c
static TupleTableSlot *
SeqNext(SeqScanState *node)
{
    TableScanDesc scandesc;
    EState *estate;
    ScanDirection direction;
    TupleTableSlot *slot;

    // Get scan information from estate and scan state
    scandesc = node->ss.ss_currentScanDesc;
    estate = node->ss.ps.state;
    direction = estate->es_direction;
    slot = node->ss.ss_ScanTupleSlot;

    // Initialize scan descriptor if needed (non-parallel or serial execution)
    if (scandesc == NULL) {
        scandesc = table_beginscan(node->ss.ss_currentRelation,
                                   estate->es_snapshot,
                                   0, NULL);
        node->ss.ss_currentScanDesc = scandesc;
    }

    // Get next tuple from table
    if (table_scan_getnextslot(scandesc, direction, slot))
        return slot;
    return NULL;
}
```