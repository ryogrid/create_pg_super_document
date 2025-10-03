# heap_fetch_toast_slice

## Location
[src/backend/access/heap/heaptoast.c:626-793](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heaptoast.c#L626-L793)

## Overview
Fetches a specified slice (byte range) from a TOAST value stored in chunks across a heap table, assembling the requested portion into a result buffer.

## Definition

```c
void
heap_fetch_toast_slice(Relation toastrel, Oid valueid, int32 attrsize,
					   int32 sliceoffset, int32 slicelength,
					   struct varlena *result)
```
## Detailed Description
This function efficiently retrieves a contiguous byte slice from a large TOAST (The Oversized-Attribute Storage Technique) value without having to fetch the entire value. TOAST values are stored as multiple chunks in a separate toast table, and this function calculates which chunks contain the requested slice, fetches only those chunks, and copies the relevant portions to the result buffer.

The function performs the following key operations:
1. Opens indexes on the toast relation for efficient chunk lookup
2. Calculates which chunks (startchunk to endchunk) contain the requested slice
3. Sets up scan keys to query the toast index by valueid and chunk sequence numbers
4. Scans chunks in order, validating chunk sequence numbers and sizes
5. Copies only the relevant byte ranges from each chunk to the result buffer
6. Performs extensive error checking for data corruption and missing chunks

## Parameters / Member Variables
- `toastrel`: The relation (table) containing the TOAST chunks to be fetched from
- `valueid`: Object identifier that uniquely identifies the specific TOAST value
- `attrsize`: Total size in bytes of the complete TOAST value (all chunks combined)
- `sliceoffset`: Starting byte offset within the TOAST value from which to begin fetching
- `slicelength`: Number of bytes to fetch from the TOAST value starting at sliceoffset
- `*result`: Pre-allocated varlena structure where the fetched slice data will be written
## Dependencies
- Functions called/Symbols referenced:
  - [toast_open_indexes](../t/toast_open_indexes.md)
  - [init_toast_snapshot](../i/init_toast_snapshot.md)
  - [systable_beginscan_ordered](../s/systable_beginscan_ordered.md)
  - [systable_getnext_ordered](../s/systable_getnext_ordered.md)
  - [systable_endscan_ordered](../s/systable_endscan_ordered.md)
  - [toast_close_indexes](../t/toast_close_indexes.md)
  - [fastgetattr](../f/fastgetattr.md)
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - VARATT_IS_EXTENDED
  - VARATT_IS_SHORT
  - VARSIZE/VARSIZE_SHORT
  - VARDATA/VARDATA_SHORT
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md) (sampling functionality)
  - Functions working with TOAST_MAX_CHUNK_SIZE

## Notes and Other Information
- Uses TOAST_MAX_CHUNK_SIZE (typically 1996 bytes) as the standard chunk size for calculations
- Implements comprehensive error checking including validation of chunk sequence numbers, sizes, and completeness
- Optimizes scan keys based on slice requirements: single chunk gets equality condition, multiple chunks use range conditions
- Handles both regular and short varlena headers in toast chunks
- Uses ordered system table scans to ensure chunks are processed in sequence
- Provides detailed error messages with corruption details for debugging toast table issues
- Critical for PostgreSQL's ability to efficiently work with large column values without reading entire objects

## Simplified Source

```c
void
heap_fetch_toast_slice(Relation toastrel, Oid valueid, int32 attrsize,
                       int32 sliceoffset, int32 slicelength,
                       struct varlena *result)
{
    Relation *toastidxs;
    ScanKeyData toastkey[3];
    int nscankeys;
    SysScanDesc toastscan;
    HeapTuple ttup;
    int32 expectedchunk;
    int32 totalchunks = ((attrsize - 1) / TOAST_MAX_CHUNK_SIZE) + 1;
    int startchunk = sliceoffset / TOAST_MAX_CHUNK_SIZE;
    int endchunk = (sliceoffset + slicelength - 1) / TOAST_MAX_CHUNK_SIZE;
    int num_indexes, validIndex;
    SnapshotData SnapshotToast;

    // Open toast table indexes
    validIndex = toast_open_indexes(toastrel, AccessShareLock, &toastidxs, &num_indexes);

    // Set up scan keys based on chunk range needed
    ScanKeyInit(&toastkey[0], (AttrNumber) 1, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(valueid));

    if (startchunk == 0 && endchunk == totalchunks - 1) {
        nscankeys = 1;  // Fetch all chunks
    } else if (startchunk == endchunk) {
        // Single chunk
        ScanKeyInit(&toastkey[1], (AttrNumber) 2, BTEqualStrategyNumber, F_INT4EQ,
                    Int32GetDatum(startchunk));
        nscankeys = 2;
    } else {
        // Range of chunks
        ScanKeyInit(&toastkey[1], (AttrNumber) 2, BTGreaterEqualStrategyNumber, F_INT4GE,
                    Int32GetDatum(startchunk));
        ScanKeyInit(&toastkey[2], (AttrNumber) 2, BTLessEqualStrategyNumber, F_INT4LE,
                    Int32GetDatum(endchunk));
        nscankeys = 3;
    }

    // Begin ordered scan of toast chunks
    init_toast_snapshot(&SnapshotToast);
    toastscan = systable_beginscan_ordered(toastrel, toastidxs[validIndex],
                                          &SnapshotToast, nscankeys, toastkey);

    // Read chunks in sequence and copy relevant portions to result
    expectedchunk = startchunk;
    while ((ttup = systable_getnext_ordered(toastscan, ForwardScanDirection)) != NULL) {
        int32 curchunk, chunksize;
        char *chunkdata;
        bool isnull;

        // Extract chunk number and data
        curchunk = DatumGetInt32(fastgetattr(ttup, 2, toastrel->rd_att, &isnull));
        Pointer chunk = DatumGetPointer(fastgetattr(ttup, 3, toastrel->rd_att, &isnull));

        // Handle different varlena formats
        if (!VARATT_IS_EXTENDED(chunk)) {
            chunksize = VARSIZE(chunk) - VARHDRSZ;
            chunkdata = VARDATA(chunk);
        } else if (VARATT_IS_SHORT(chunk)) {
            chunksize = VARSIZE_SHORT(chunk) - VARHDRSZ_SHORT;
            chunkdata = VARDATA_SHORT(chunk);
        } else {
            elog(ERROR, "found toasted toast chunk for toast value %u", valueid);
        }

        // Validate chunk sequence and size
        if (curchunk != expectedchunk)
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                           errmsg_internal("unexpected chunk number %d (expected %d)",
                                         curchunk, expectedchunk)));

        // Copy relevant portion of chunk to result buffer
        int32 chcpystrt = (curchunk == startchunk) ? sliceoffset % TOAST_MAX_CHUNK_SIZE : 0;
        int32 chcpyend = (curchunk == endchunk) ?
                        (sliceoffset + slicelength - 1) % TOAST_MAX_CHUNK_SIZE :
                        chunksize - 1;

        memcpy(VARDATA(result) + (curchunk * TOAST_MAX_CHUNK_SIZE - sliceoffset) + chcpystrt,
               chunkdata + chcpystrt, (chcpyend - chcpystrt) + 1);

        expectedchunk++;
    }

    // Cleanup
    systable_endscan_ordered(toastscan);
    toast_close_indexes(toastidxs, num_indexes, AccessShareLock);
}
```