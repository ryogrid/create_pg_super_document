# IndexScanDesc

## Location
src/include/access/genam.h: 90 - 90

## Overview
IndexScanDesc is a pointer type to IndexScanDescData structure that represents an index scan descriptor, serving as the primary interface for managing index scan operations in PostgreSQL.

## Definition


The actual structure definition (IndexScanDescData) contains:


## Detailed Description
IndexScanDesc serves as the central data structure for index scanning operations in PostgreSQL. It supports both amgettuple-based and amgetbitmap-based index scans, with some fields being relevant only for specific scan types. The structure encapsulates all necessary information for conducting index scans, including scan parameters, tuple handling, snapshot management, and parallel scan coordination.

The structure is designed to work with both regular index scans and index-only scans. For index-only scans, either xs_itup/xs_itupdesc or xs_hitup/xs_hitupdesc (or both) must be filled by successful amgettuple calls to provide the data returned by the scan.

## Parameters / Member Variables
- : Heap relation descriptor, or NULL for index-only scans
- : Index relation descriptor being scanned
- : Snapshot for visibility determination during scan
- : Number of index qualifier conditions (WHERE clauses)
- : Number of ordering operators (ORDER BY clauses)
- : Array of index qualifier descriptors (scan keys)
- : Array of ordering operator descriptors
- : Flag indicating caller requests index tuples
- : Flag to unregister snapshot at scan end
- : Signal that last-returned tuple is dead
- : Flag to not return killed entries
- : Prevents killing/seeing killed tuples during recovery
- : Access-method-specific private state
- : Index tuple returned by access method
- : Row type descriptor for xs_itup
- : Index data returned as HeapTuple format
- : Row type descriptor for xs_hitup
- : Item pointer to heap tuple result
- : Flag indicating more potential results exist
- : Index fetch table data for heap access
- : Flag indicating scan keys must be rechecked
- : VALUES for ORDER BY expressions of last returned tuple
- : NULL flags for ORDER BY expressions
- : Flag indicating ORDER BY values need rechecking
- : Parallel index scan information in shared memory

## Dependencies
- Functions called/Symbols referenced:
  - [IndexScanDescData](IndexScanDescData.md) (the actual structure definition)
  - [SnapshotData](../S/SnapshotData.md) (for snapshot management)
  - [ScanKeyData](../S/ScanKeyData.md) (for scan key handling)
  - [IndexTuple](IndexTuple.md)/HeapTuple (for tuple results)
  - [ParallelIndexScanDescData](../P/ParallelIndexScanDescData.md) (for parallel scans)
- Called from (representative examples):
  - [btbeginscan](../b/btbeginscan.md)/btrescan/btendscan (B-tree scans)
  - [ginbeginscan](../g/ginbeginscan.md)/ginrescan/ginendscan (GIN scans)
  - [gistbeginscan](../g/gistbeginscan.md)/gistrescan/gistendscan (GiST scans)
  - [hashbeginscan](../h/hashbeginscan.md)/hashrescan/hashendscan (Hash scans)
  - [spgbeginscan](../s/spgbeginscan.md)/spgrescan/spgendscan (SP-GiST scans)
  - [index_beginscan](../i/index_beginscan.md)/index_rescan/index_endscan (generic scan functions)

## Notes and Other Information
- This is defined in src/include/access/genam.h as a pointer typedef, with the actual structure in src/include/access/relscan.h
- Used extensively across all index access methods in PostgreSQL
- Supports both regular index scans (returning heap TIDs) and index-only scans (returning index tuple data)
- The structure accommodates parallel scanning through the parallel_scan field
- ORDER BY support allows for nearest-neighbor searches and sorted index scans
- The opaque field allows each index access method to maintain its own private scan state
- Critical for executor nodes like IndexScan, IndexOnlyScan, and BitmapIndexScan
- The kill_prior_tuple mechanism helps optimize repeated scans by marking dead tuples