# inv_read

## Location
[src/backend/storage/large_object/inv_api.c:488-580](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/large_object/inv_api.c#L488-L580)

## Overview
Reads data from a PostgreSQL large object starting at the current position, similar to fread() in standard C file I/O, handling page-based storage and potential gaps (holes) in the large object.

## Definition
```c
int inv_read(LargeObjectDesc *obj_desc, char *buf, int nbytes)
```

## Detailed Description
The `inv_read` function reads up to `nbytes` bytes from a large object into a buffer, starting from the current read position stored in the large object descriptor. This function is part of PostgreSQL's large object (BLOB) API and serves as the equivalent of the standard C library's `fread()` function for large objects.

The function handles PostgreSQL's page-based storage system for large objects, where data is stored in chunks of LOBLKSIZE (typically 2KB). It efficiently handles sparse large objects by detecting missing pages ("holes") and filling them with zeros. The function uses a system catalog scan to read pages in order, starting from the page containing the current offset.

Key behaviors include:
- Permission checking to ensure read access
- Handling of missing pages as zero-filled holes
- Sequential reading across multiple pages
- Automatic position tracking in the object descriptor

## Parameters / Member Variables
- `obj_desc`: Pointer to a LargeObjectDesc structure representing an open large object. Must have read permissions (IFS_RDLOCK flag set).
- `buf`: Buffer to store the read data. Must be at least `nbytes` bytes long.
- `nbytes`: Maximum number of bytes to read from the large object.

## Dependencies
- Functions called/Symbols referenced:
  - `PointerIsValid` (pointer validation macro)
  - [open_lo_relation](../o/open_lo_relation.md) (opens large object system relation)
  - [systable_beginscan_ordered](../s/systable_beginscan_ordered.md), `systable_getnext_ordered`, `systable_endscan_ordered` (system catalog scanning)
  - [getdatafield](../g/getdatafield.md) (extracts data from large object tuple)
  - `MemSet` (memory zeroing for holes)
  - `LOBLKSIZE` (large object block size constant)
- Called from (representative examples):
  - [lo_read](../l/lo_read.md) (user-facing read function)
  - [be_lo_export](../b/be_lo_export.md) (large object export functionality)
  - [lo_get_fragment_internal](../l/lo_get_fragment_internal.md) (internal fragment retrieval)

## Notes and Other Information
- Returns the actual number of bytes read (may be less than requested)
- Returns 0 if nbytes <= 0 or no data available
- Automatically handles sparse large objects by zero-filling missing pages
- Updates the object descriptor's offset to reflect the new position after reading
- Requires IFS_RDLOCK permission flag to be set in the object descriptor
- Uses ordered index scans for efficient sequential access to large object pages
- Handles variable-length data fields with proper memory management

## Simplified Source

```c
int inv_read(LargeObjectDesc *obj_desc, char *buf, int nbytes) {
    int nread = 0;
    int32 pageno = (int32) (obj_desc->offset / LOBLKSIZE);
    ScanKeyData skey[2];

    Assert(PointerIsValid(obj_desc));
    Assert(buf != NULL);

    // Check read permission
    if ((obj_desc->flags & IFS_RDLOCK) == 0)
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                       errmsg("permission denied for large object %u", obj_desc->id)));

    if (nbytes <= 0)
        return 0;

    // Ensure large object relations are open
    open_lo_relation();

    // Set up scan keys to find pages starting from current position
    ScanKeyInit(&skey[0], Anum_pg_largeobject_loid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(obj_desc->id));

    ScanKeyInit(&skey[1], Anum_pg_largeobject_pageno,
                BTGreaterEqualStrategyNumber, F_INT4GE,
                Int32GetDatum(pageno));

    // Begin ordered scan of large object pages
    SysScanDesc sd = systable_beginscan_ordered(lo_heap_r, lo_index_r,
                                              obj_desc->snapshot, 2, skey);

    // Process each page in sequence
    while ((tuple = systable_getnext_ordered(sd, ForwardScanDirection)) != NULL) {
        Form_pg_largeobject data;
        bytea *datafield;
        bool pfreeit;

        if (HeapTupleHasNulls(tuple))
            elog(ERROR, "null field found in pg_largeobject");

        data = (Form_pg_largeobject) GETSTRUCT(tuple);

        // Handle holes (missing pages) by filling with zeros
        uint64 pageoff = ((uint64) data->pageno) * LOBLKSIZE;
        if (pageoff > obj_desc->offset) {
            int64 n = pageoff - obj_desc->offset;
            n = (n <= (nbytes - nread)) ? n : (nbytes - nread);
            MemSet(buf + nread, 0, n);
            nread += n;
            obj_desc->offset += n;
        }

        // Read data from current page if more bytes needed
        if (nread < nbytes) {
            int64 off = obj_desc->offset - pageoff;
            int len;

            getdatafield(data, &datafield, &len, &pfreeit);
            if (len > off) {
                int64 n = len - off;
                n = (n <= (nbytes - nread)) ? n : (nbytes - nread);
                memcpy(buf + nread, VARDATA(datafield) + off, n);
                nread += n;
                obj_desc->offset += n;
            }
            if (pfreeit)
                pfree(datafield);
        }

        if (nread >= nbytes)
            break;
    }

    systable_endscan_ordered(sd);
    return nread;
}
```