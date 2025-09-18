# IndexScanDescData

## Location
src/include/access/relscan.h: 114 - 167

## Overview
A comprehensive structure that describes an index scan operation, containing all state and metadata needed for both amgettuple-based and amgetbitmap-based index scans in PostgreSQL.

## Definition
```c
typedef struct IndexScanDescData
{
    /* scan parameters */
    Relation        heapRelation;       /* heap relation descriptor, or NULL */
    Relation        indexRelation;      /* index relation descriptor */
    struct SnapshotData *xs_snapshot;   /* snapshot to see */
    int             numberOfKeys;       /* number of index qualifier conditions */
    int             numberOfOrderBys;   /* number of ordering operators */
    struct ScanKeyData *keyData;        /* array of index qualifier descriptors */
    struct ScanKeyData *orderByData;    /* array of ordering op descriptors */
    bool            xs_want_itup;       /* caller requests index tuples */
    bool            xs_temp_snap;       /* unregister snapshot at scan end? */

    /* signaling to index AM about killing index tuples */
    bool            kill_prior_tuple;   /* last-returned tuple is dead */
    bool            ignore_killed_tuples; /* do not return killed entries */
    bool            xactStartedInRecovery; /* prevents killing/seeing killed tuples */

    /* index access method's private state */
    void           *opaque;             /* access-method-specific info */

    /* tuple results for index-only scans */
    IndexTuple      xs_itup;            /* index tuple returned by AM */
    struct TupleDescData *xs_itupdesc;  /* rowtype descriptor of xs_itup */
    HeapTuple       xs_hitup;           /* index data returned by AM, as HeapTuple */
    struct TupleDescData *xs_hitupdesc; /* rowtype descriptor of xs_hitup */

    ItemPointerData xs_heaptid;         /* result */
    bool            xs_heap_continue;   /* T if must keep walking, potential further results */
    IndexFetchTableData *xs_heapfetch;

    bool            xs_recheck;         /* T means scan keys must be rechecked */

    /* ORDER BY support */
    Datum          *xs_orderbyvals;
    bool           *xs_orderbynulls;
    bool            xs_recheckorderby;

    /* parallel index scan information, in shared memory */
    struct ParallelIndexScanDescData *parallel_scan;
} IndexScanDescData;
```

## Detailed Description
IndexScanDescData is the central structure for managing index scan operations in PostgreSQL. It serves as a unified interface for both amgettuple-based scans (which return one tuple at a time) and amgetbitmap-based scans (which return bitmaps of matching tuples). The structure encapsulates all necessary state including scan parameters, tuple killing optimization hints, access method-specific data, result storage for index-only scans, and support for parallel scanning. This comprehensive design allows the same structure to handle various scan types while providing flexibility for different index access methods to store their private state.

## Parameters / Member Variables
- `heapRelation`: Relation - Heap relation descriptor, NULL for index-only scans
- `indexRelation`: Relation - Index relation descriptor being scanned
- `xs_snapshot`: SnapshotData* - Snapshot defining tuple visibility for the scan
- `numberOfKeys`: int - Number of index qualifier conditions to apply
- `numberOfOrderBys`: int - Number of ordering operators for sorted results
- `keyData`: ScanKeyData* - Array of index qualifier descriptors defining scan conditions
- `orderByData`: ScanKeyData* - Array of ordering operator descriptors for result sorting
- `xs_want_itup`: bool - Whether caller wants index tuples returned
- `xs_temp_snap`: bool - Whether to unregister snapshot at scan end
- `kill_prior_tuple`: bool - Indicates last-returned tuple is dead (optimization hint)
- `ignore_killed_tuples`: bool - Whether to skip returning killed entries
- `xactStartedInRecovery`: bool - Prevents killing/seeing killed tuples during recovery
- `opaque`: void* - Access method-specific private state and information
- `xs_itup`: IndexTuple - Index tuple returned by access method in index-only scans
- `xs_itupdesc`: TupleDescData* - Row type descriptor for xs_itup
- `xs_hitup`: HeapTuple - Index data formatted as HeapTuple for index-only scans
- `xs_hitupdesc`: TupleDescData* - Row type descriptor for xs_hitup
- `xs_heaptid`: ItemPointerData - Heap tuple identifier result from index lookup
- `xs_heap_continue`: bool - Indicates more potential results exist requiring continued walking
- `xs_heapfetch`: IndexFetchTableData* - State for fetching heap tuples via index
- `xs_recheck`: bool - Whether scan keys must be rechecked against heap tuple
- `xs_orderbyvals`: Datum* - ORDER BY expression values from last returned tuple
- `xs_orderbynulls`: bool* - NULL indicators for ORDER BY values
- `xs_recheckorderby`: bool - Whether ORDER BY values need rechecking
- `parallel_scan`: ParallelIndexScanDescData* - Shared state for parallel index scans

## Dependencies
- Functions called/Symbols referenced:
  - SnapshotData
  - ScanKeyData  
  - TupleDescData
  - IndexFetchTableData
  - ParallelIndexScanDescData
  - IndexTuple
  - HeapTuple
  - ItemPointerData
  - Datum
- Called from (representative examples):
  - RelationGetIndexScan
  - IndexScanDesc (typedef)
  - SysScanDescData
  - IndexScanState
  - IndexOnlyScanState
  - BitmapIndexScanState

## Notes and Other Information
- Unified structure supporting both tuple-at-a-time and bitmap index scan modes
- Contains optimization features like tuple killing hints to improve performance
- Supports index-only scans with dual tuple format options (index format vs heap format)
- Includes comprehensive ORDER BY support for sorted index scans
- Integrates with PostgreSQL's parallel query execution framework
- The opaque field allows different index access methods to maintain their specific state
- Critical component of PostgreSQL's pluggable index access method architecture
- Used extensively throughout the query execution engine for all index-based operations