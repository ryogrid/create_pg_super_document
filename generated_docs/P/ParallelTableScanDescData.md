# ParallelTableScanDescData

## Location
[src/include/access/relscan.h:63-69](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/relscan.h#L63-L69)

## Overview
ParallelTableScanDescData defines the shared state structure for parallel table scans, containing information necessary to coordinate scanning across multiple backend processes.

## Definition
```c
typedef struct ParallelTableScanDescData
{
    Oid         phs_relid;          /* OID of relation to scan */
    bool        phs_syncscan;       /* report location to syncscan logic? */
    bool        phs_snapshot_any;   /* SnapshotAny, not phs_snapshot_data? */
    Size        phs_snapshot_off;   /* data for snapshot */
} ParallelTableScanDescData;
```

## Detailed Description
ParallelTableScanDescData serves as the shared state structure for coordinating parallel table scans among multiple worker processes. This structure is placed in shared memory and contains the essential information that all participating backends need to properly initialize their individual TableScanDesc objects and coordinate their scanning efforts. Each backend has its own private TableScanDesc that contains a pointer to this shared structure, ensuring that all workers operate on the same relation with consistent scan parameters.

## Parameters / Member Variables
- `phs_relid`: Object identifier (OID) of the relation being scanned, allowing workers to open the correct table
- `phs_syncscan`: Boolean flag indicating whether to report scan location to the synchronized scan logic for coordination with other concurrent scans
- `phs_snapshot_any`: Boolean flag indicating whether to use SnapshotAny instead of the snapshot data stored at phs_snapshot_off
- `phs_snapshot_off`: Offset to the snapshot data within the shared memory structure, providing access to the snapshot for tuple visibility checking

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)
- Called from (representative examples):
  - [TableScanDescData](../T/TableScanDescData.md) (src/include/access/relscan.h:49)
  - [ParallelTableScanDesc](ParallelTableScanDesc.md) (src/include/access/relscan.h:70)
  - [ParallelBlockTableScanDescData](ParallelBlockTableScanDescData.md) (src/include/access/relscan.h:77)

## Notes and Other Information
This structure is defined in src/include/access/relscan.h (lines 63-69) and provides the foundation for PostgreSQL's parallel table scanning capability. The structure is designed to be extensible, with access method-specific implementations like ParallelBlockTableScanDescData building upon this base. The snapshot handling allows for consistent visibility across all parallel workers, while the syncscan integration helps optimize I/O patterns when multiple scans are active simultaneously.