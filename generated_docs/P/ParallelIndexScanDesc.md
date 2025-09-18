# ParallelIndexScanDesc

## Location
[src/include/access/genam.h:93-114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/genam.h#L93-L114)

## Overview
ParallelIndexScanDesc is a pointer type to ParallelIndexScanDescData structure that represents the shared state needed for parallel index scanning operations in PostgreSQL.

## Definition


## Detailed Description
ParallelIndexScanDesc is a generic structure used for coordinating parallel index scans across multiple worker processes. It serves as a shared memory structure that contains essential information needed by all participating workers in a parallel index scan operation. The structure includes relation and index identifiers, an offset to access method-specific data, and a flexible array member to hold snapshot data that ensures consistent visibility across all parallel workers.

## Parameters / Member Variables
- : Object identifier of the relation being scanned
- : Object identifier of the specific index being used for the scan
- : Byte offset to locate access method-specific parallel scan structures
- : Flexible array member containing serialized snapshot data for maintaining transaction isolation across parallel workers

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelIndexScanDescData](ParallelIndexScanDescData.md)
  - Oid
  - Size
  - FLEXIBLE_ARRAY_MEMBER

- Called from (representative examples):
  - [index_beginscan_internal](../i/index_beginscan_internal.md)
  - [index_parallelscan_initialize](../i/index_parallelscan_initialize.md)
  - [index_beginscan_parallel](../i/index_beginscan_parallel.md)
  - [ExecIndexScanInitializeDSM](../E/ExecIndexScanInitializeDSM.md)
  - [ExecIndexOnlyScanInitializeDSM](../E/ExecIndexOnlyScanInitializeDSM.md)
  - [btparallelrescan](../b/btparallelrescan.md)

## Notes and Other Information
- This structure is part of PostgreSQL's parallel query execution framework
- The ps_snapshot_data field uses a flexible array member to accommodate variable-length snapshot information
- Different index access methods (like B-tree) may extend this structure with their own parallel scan state
- The structure is typically allocated in shared memory to be accessible by all parallel worker processes
- Used primarily by the executor nodes for parallel index scans and index-only scans