# TableScanDescData

## Location
[src/include/access/relscan.h:31-51](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/relscan.h#L31-L51)

## Overview
TableScanDescData is the generic base descriptor structure for table scans that needs to be embedded in the scans of individual access methods (AMs).

## Definition
```c
typedef struct TableScanDescData
{
    /* scan parameters */
    Relation            rs_rd;          /* heap relation descriptor */
    struct SnapshotData *rs_snapshot;   /* snapshot to see */
    int                 rs_nkeys;       /* number of scan keys */
    struct ScanKeyData  *rs_key;        /* array of scan key descriptors */

    /* Range of ItemPointers for table_scan_getnextslot_tidrange() to scan. */
    ItemPointerData rs_mintid;
    ItemPointerData rs_maxtid;

    /*
     * Information about type and behaviour of the scan, a bitmask of members
     * of the ScanOptions enum (see tableam.h).
     */
    uint32              rs_flags;

    struct ParallelTableScanDescData *rs_parallel;  /* parallel scan
                                                     * information */
} TableScanDescData;
```

## Detailed Description
TableScanDescData serves as the base class for all table scan descriptors in PostgreSQL. This structure contains the essential information needed to perform table scans across different access methods. It encapsulates scan parameters, snapshot information, scan keys for filtering, and support for parallel scanning. The structure is designed to be embedded within access method-specific scan descriptors, providing a common interface for table scanning operations.

## Parameters / Member Variables
- `rs_rd`: Relation descriptor for the heap relation being scanned
- `rs_snapshot`: Pointer to the snapshot data structure that defines which tuple versions are visible during the scan
- `rs_nkeys`: Number of scan key descriptors in the rs_key array
- `rs_key`: Array of scan key descriptors used for filtering tuples during the scan
- `rs_mintid`: Minimum ItemPointer value for bounded scans using table_scan_getnextslot_tidrange()
- `rs_maxtid`: Maximum ItemPointer value for bounded scans using table_scan_getnextslot_tidrange()
- `rs_flags`: Bitmask containing scan options and behavior flags (see ScanOptions enum in tableam.h)
- `rs_parallel`: Pointer to parallel table scan descriptor data for coordinating parallel scan operations

## Dependencies
- Functions called/Symbols referenced:
  - [SnapshotData](../S/SnapshotData.md)
  - [ParallelTableScanDescData](../P/ParallelTableScanDescData.md)
- Called from (representative examples):
  - [HeapScanDescData](../H/HeapScanDescData.md) (src/include/access/heapam.h:55)
  - [TableScanDesc](TableScanDesc.md) (src/include/access/relscan.h:52)
  - [SysScanDescData](../S/SysScanDescData.md) (src/include/access/relscan.h:185)
  - [ScanState](../S/ScanState.md) (src/include/nodes/execnodes.h:1568)

## Notes and Other Information
This structure is defined in src/include/access/relscan.h (lines 31-51) and serves as the foundation for PostgreSQL's table access method abstraction layer. Access method implementations should embed this structure in their own scan descriptor types to maintain compatibility with the table AM interface. The parallel scanning capability allows multiple worker processes to cooperatively scan large tables efficiently.