# inv_write

## Location
[src/backend/storage/large_object/inv_api.c:581-777](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/large_object/inv_api.c#L581-L777)

## Overview
Writes data to a PostgreSQL large object starting at the current position, similar to fwrite() in standard C file I/O, handling page-based storage, size limits, and both updating existing pages and creating new ones.

## Definition
```c
int inv_write(LargeObjectDesc *obj_desc, const char *buf, int nbytes)
```

## Detailed Description
The `inv_write` function writes up to `nbytes` bytes from a buffer to a large object, starting from the current write position stored in the large object descriptor. This function is part of PostgreSQL's large object (BLOB) API and serves as the equivalent of the standard C library's `fwrite()` function for large objects.

The function handles PostgreSQL's page-based storage system for large objects, where data is stored in chunks of LOBLKSIZE (typically 2KB). It efficiently manages both updating existing pages and creating new pages as needed. The function enforces size limits and maintains proper transaction semantics.

Key behaviors include:
- Permission checking to ensure write access (IFS_WRLOCK flag)
- Size limit enforcement (MAX_LARGE_OBJECT_SIZE)
- Efficient page-based storage management
- Handling of sparse writes with zero-filled holes
- Transaction-safe catalog updates with proper indexing
- Command counter increment for transaction visibility

The function works by:
1. Scanning existing pages starting from the target page
2. For existing pages: loading current data, filling holes, merging new data, updating tuple
3. For new pages: zero-filling holes, inserting new data, creating new tuple
4. Maintaining proper catalog indexes and transaction visibility

## Parameters / Member Variables
- `obj_desc`: Pointer to a LargeObjectDesc structure representing an open large object. Must have write permissions (IFS_WRLOCK flag set).
- `buf`: Buffer containing the data to write. Must be at least `nbytes` bytes long.
- `nbytes`: Number of bytes to write to the large object.

## Dependencies
- Functions called/Symbols referenced:
  - `PointerIsValid` (pointer validation macro)
  - [open_lo_relation](../o/open_lo_relation.md), `CatalogOpenIndexes`, `CatalogCloseIndexes` (catalog management)
  - [systable_beginscan_ordered](../s/systable_beginscan_ordered.md), `systable_getnext_ordered`, `systable_endscan_ordered` (system catalog scanning)
  - [getdatafield](../g/getdatafield.md) (extracts data from large object tuple)
  - [heap_modify_tuple](../h/heap_modify_tuple.md), `heap_form_tuple`, `heap_freetuple` (tuple management)
  - [CatalogTupleUpdateWithInfo](../C/CatalogTupleUpdateWithInfo.md), `CatalogTupleInsertWithInfo` (catalog updates)
  - `[CommandCounterIncrement](../C/CommandCounterIncrement.md)` (transaction visibility)
  - `MemSet` (memory zeroing for holes)
  - `MAX_LARGE_OBJECT_SIZE`, `LOBLKSIZE` (size constants)
- Called from (representative examples):
  - [lo_write](../l/lo_write.md) (user-facing write function)
  - [lo_import_internal](../l/lo_import_internal.md) (large object import functionality)
  - [be_lo_from_bytea](../b/be_lo_from_bytea.md) (bytea to large object conversion)
  - [be_lo_put](../b/be_lo_put.md) (large object data replacement)

## Notes and Other Information
- Returns the actual number of bytes written (may be less than requested due to size limits)
- Returns 0 if nbytes <= 0
- Enforces MAX_LARGE_OBJECT_SIZE limit and raises error if exceeded
- Automatically handles sparse writes by zero-filling holes in pages
- Updates the object descriptor's offset to reflect the new position after writing
- Requires IFS_WRLOCK permission flag to be set in the object descriptor
- Uses ordered index scans and proper catalog management for consistency
- Maintains transaction safety with proper tuple versioning and command counter increments
- Efficiently handles both partial page updates and full page writes
- Memory management includes proper cleanup of temporary data structures

## Simplified Source

```c
int inv_write(LargeObjectDesc *obj_desc, const char *buf, int nbytes) {
    int nwritten = 0;
    int32 pageno = (int32) (obj_desc->offset / LOBLKSIZE);
    ScanKeyData skey[2];
    SysScanDesc sd;
    HeapTuple oldtuple;
    Form_pg_largeobject olddata;
    bool neednextpage;

    // Work buffer for building page data
    union {
        bytea hdr;
        char data[LOBLKSIZE + VARHDRSZ];
        int32 align_it;
    } workbuf;
    char *workb = VARDATA(&workbuf.hdr);

    Assert(PointerIsValid(obj_desc));
    Assert(buf != NULL);

    // Check write permission
    if ((obj_desc->flags & IFS_WRLOCK) == 0)
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                       errmsg("permission denied for large object %u", obj_desc->id)));

    if (nbytes <= 0)
        return 0;

    // Check size limit
    if ((nbytes + obj_desc->offset) > MAX_LARGE_OBJECT_SIZE)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("invalid large object write request size: %d", nbytes)));

    // Ensure large object relations are open
    open_lo_relation();
    CatalogIndexState indstate = CatalogOpenIndexes(lo_heap_r);

    // Set up scan keys to find pages starting from current position
    ScanKeyInit(&skey[0], Anum_pg_largeobject_loid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(obj_desc->id));

    ScanKeyInit(&skey[1], Anum_pg_largeobject_pageno,
                BTGreaterEqualStrategyNumber, F_INT4GE,
                Int32GetDatum(pageno));

    // Begin ordered scan of large object pages
    sd = systable_beginscan_ordered(lo_heap_r, lo_index_r,
                                  obj_desc->snapshot, 2, skey);

    oldtuple = NULL;
    olddata = NULL;
    neednextpage = true;

    // Write data page by page
    while (nwritten < nbytes) {
        // Get next existing page if needed
        if (neednextpage) {
            if ((oldtuple = systable_getnext_ordered(sd, ForwardScanDirection)) != NULL) {
                if (HeapTupleHasNulls(oldtuple))
                    elog(ERROR, "null field found in pg_largeobject");
                olddata = (Form_pg_largeobject) GETSTRUCT(oldtuple);
                Assert(olddata->pageno >= pageno);
            }
            neednextpage = false;
        }

        if (olddata != NULL && olddata->pageno == pageno) {
            // Update existing page
            bytea *datafield;
            int len;
            bool pfreeit;

            // Load existing data into work buffer
            getdatafield(olddata, &datafield, &len, &pfreeit);
            memcpy(workb, VARDATA(datafield), len);
            if (pfreeit)
                pfree(datafield);

            // Fill any hole and insert new data
            int off = (int) (obj_desc->offset % LOBLKSIZE);
            if (off > len)
                MemSet(workb + len, 0, off - len);

            int n = LOBLKSIZE - off;
            n = (n <= (nbytes - nwritten)) ? n : (nbytes - nwritten);
            memcpy(workb + off, buf + nwritten, n);
            nwritten += n;
            obj_desc->offset += n;

            // Update tuple with new data
            len = (len >= off + n) ? len : off + n;
            SET_VARSIZE(&workbuf.hdr, len + VARHDRSZ);

            Datum values[Natts_pg_largeobject] = {0};
            bool nulls[Natts_pg_largeobject] = {false};
            bool replace[Natts_pg_largeobject] = {false};

            values[Anum_pg_largeobject_data - 1] = PointerGetDatum(&workbuf);
            replace[Anum_pg_largeobject_data - 1] = true;

            HeapTuple newtup = heap_modify_tuple(oldtuple, RelationGetDescr(lo_heap_r),
                                               values, nulls, replace);
            CatalogTupleUpdateWithInfo(lo_heap_r, &newtup->t_self, newtup, indstate);
            heap_freetuple(newtup);

            oldtuple = NULL;
            olddata = NULL;
            neednextpage = true;
        }
        else {
            // Create new page
            int off = (int) (obj_desc->offset % LOBLKSIZE);
            if (off > 0)
                MemSet(workb, 0, off);

            int n = LOBLKSIZE - off;
            n = (n <= (nbytes - nwritten)) ? n : (nbytes - nwritten);
            memcpy(workb + off, buf + nwritten, n);
            nwritten += n;
            obj_desc->offset += n;

            // Create new tuple
            int len = off + n;
            SET_VARSIZE(&workbuf.hdr, len + VARHDRSZ);

            Datum values[Natts_pg_largeobject] = {0};
            bool nulls[Natts_pg_largeobject] = {false};

            values[Anum_pg_largeobject_loid - 1] = ObjectIdGetDatum(obj_desc->id);
            values[Anum_pg_largeobject_pageno - 1] = Int32GetDatum(pageno);
            values[Anum_pg_largeobject_data - 1] = PointerGetDatum(&workbuf);

            HeapTuple newtup = heap_form_tuple(lo_heap_r->rd_att, values, nulls);
            CatalogTupleInsertWithInfo(lo_heap_r, newtup, indstate);
            heap_freetuple(newtup);
        }
        pageno++;
    }

    systable_endscan_ordered(sd);
    CatalogCloseIndexes(indstate);

    // Make changes visible to subsequent operations
    CommandCounterIncrement();

    return nwritten;
}
```