# inv_truncate

## Location
[src/backend/storage/large_object/inv_api.c:778-953](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/large_object/inv_api.c#L778-L953)

## Overview
Truncates a PostgreSQL large object to a specified length, similar to ftruncate() in standard C file I/O, handling page-based storage by updating the target page and deleting all pages beyond the truncation point.

## Definition
```c
void inv_truncate(LargeObjectDesc *obj_desc, int64 len)
```

## Detailed Description
The `inv_truncate` function truncates a large object to the specified length by either shortening or lengthening it. This function is part of PostgreSQL's large object (BLOB) API and serves as the equivalent of the standard C library's `ftruncate()` function for large objects.

The function handles PostgreSQL's page-based storage system for large objects, where data is stored in chunks of LOBLKSIZE (typically 2KB). It efficiently manages three scenarios:
1. Truncating within an existing page (shortens the page data)
2. Truncating in a hole (creates a new page with zero-fill up to the truncation point)
3. Truncating beyond existing data (similar to hole case)

Key behaviors include:
- Permission checking to ensure write access (IFS_WRLOCK flag)
- Length validation (must be >= 0 and <= MAX_LARGE_OBJECT_SIZE)
- Efficient page-based storage management
- Proper handling of sparse large objects with holes
- Complete removal of pages beyond the truncation point
- Transaction-safe catalog updates with proper indexing
- Command counter increment for transaction visibility

The function works by:
1. Calculating the target page number and offset within that page
2. Scanning for existing pages starting from the target page
3. If target page exists: loading data, truncating at specified offset, updating tuple
4. If target page doesn't exist: creating new page with zero-fill up to truncation point
5. Deleting all pages beyond the truncation point
6. Maintaining proper catalog indexes and transaction visibility

## Parameters / Member Variables
- `obj_desc`: Pointer to a LargeObjectDesc structure representing an open large object. Must have write permissions (IFS_WRLOCK flag set).
- `len`: Target length for the large object in bytes. Must be >= 0 and <= MAX_LARGE_OBJECT_SIZE.

## Dependencies
- Functions called/Symbols referenced:
  - `PointerIsValid` (pointer validation macro)
  - [open_lo_relation](../o/open_lo_relation.md), `CatalogOpenIndexes`, `CatalogCloseIndexes` (catalog management)
  - [systable_beginscan_ordered](../s/systable_beginscan_ordered.md), `systable_getnext_ordered`, `systable_endscan_ordered` (system catalog scanning)
  - [getdatafield](../g/getdatafield.md) (extracts data from large object tuple)
  - [heap_modify_tuple](../h/heap_modify_tuple.md), `heap_form_tuple`, `heap_freetuple` (tuple management)
  - [CatalogTupleUpdateWithInfo](../C/CatalogTupleUpdateWithInfo.md), `CatalogTupleInsertWithInfo`, `CatalogTupleDelete` (catalog updates)
  - `[CommandCounterIncrement](../C/CommandCounterIncrement.md)` (transaction visibility)
  - `MemSet` (memory zeroing for holes)
  - `MAX_LARGE_OBJECT_SIZE`, `LOBLKSIZE` (size constants)
- Called from (representative examples):
  - [lo_truncate_internal](../l/lo_truncate_internal.md) (user-facing truncate function)

## Notes and Other Information
- Returns void (no return value)
- Validates length parameter (0 <= len <= MAX_LARGE_OBJECT_SIZE)
- Requires IFS_WRLOCK permission flag to be set in the object descriptor
- Efficiently handles sparse large objects by creating minimal pages with zero-fill
- Properly deletes all pages beyond the truncation point to reclaim storage
- Uses ordered index scans and proper catalog management for consistency
- Maintains transaction safety with proper tuple versioning and command counter increments
- Handles both shortening (removing data) and lengthening (adding zero-filled holes) operations
- Memory management includes proper cleanup of temporary data structures
- Uses internal error messages for length validation to avoid exposing internal format strings
- The object's current position (offset) is not modified by this operation

## Simplified Source

```c
void inv_truncate(LargeObjectDesc *obj_desc, int64 len) {
    int32 target_page = len / LOBLKSIZE;
    int32 offset_in_page = len % LOBLKSIZE;

    // Validate parameters
    if ((obj_desc->flags & IFS_WRLOCK) == 0)
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                       errmsg("permission denied for large object %u", obj_desc->id)));

    if (len < 0 || len > MAX_LARGE_OBJECT_SIZE)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg_internal("invalid large object truncation target")));

    // Open large object relation and indexes
    open_lo_relation();
    CatalogIndexState indstate = CatalogOpenIndexes(lo_heap_r);

    // Scan for pages starting from target page
    ScanKeyData skey[2];
    ScanKeyInit(&skey[0], Anum_pg_largeobject_loid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(obj_desc->id));
    ScanKeyInit(&skey[1], Anum_pg_largeobject_pageno, BTGreaterEqualStrategyNumber,
                F_INT4GE, Int32GetDatum(target_page));

    SysScanDesc sd = systable_beginscan_ordered(lo_heap_r, lo_index_r,
                                                obj_desc->snapshot, 2, skey);

    // Get first page at or after truncation point
    HeapTuple oldtuple = systable_getnext_ordered(sd, ForwardScanDirection);
    Form_pg_largeobject olddata = NULL;
    if (oldtuple != NULL) {
        olddata = (Form_pg_largeobject) GETSTRUCT(oldtuple);
    }

    // Handle the truncation target page
    if (olddata != NULL && olddata->pageno == target_page) {
        // Truncate existing page at target position
        char workbuf[LOBLKSIZE + VARHDRSZ];
        bytea *datafield;
        int pagelen;
        bool pfreeit;

        // Load existing data and truncate
        getdatafield(olddata, &datafield, &pagelen, &pfreeit);
        memcpy(VARDATA(workbuf), VARDATA(datafield), pagelen);
        if (pfreeit) pfree(datafield);

        // Zero-fill any hole if needed
        if (offset_in_page > pagelen)
            MemSet(VARDATA(workbuf) + pagelen, 0, offset_in_page - pagelen);

        SET_VARSIZE(workbuf, offset_in_page + VARHDRSZ);

        // Update existing tuple with truncated data
        Datum values[Natts_pg_largeobject] = {0};
        bool nulls[Natts_pg_largeobject] = {false};
        bool replace[Natts_pg_largeobject] = {false};

        values[Anum_pg_largeobject_data - 1] = PointerGetDatum(workbuf);
        replace[Anum_pg_largeobject_data - 1] = true;

        HeapTuple newtup = heap_modify_tuple(oldtuple, RelationGetDescr(lo_heap_r),
                                           values, nulls, replace);
        CatalogTupleUpdateWithInfo(lo_heap_r, &newtup->t_self, newtup, indstate);
        heap_freetuple(newtup);
    } else {
        // Create new page if we're in a hole or beyond existing data
        if (olddata != NULL && olddata->pageno > target_page) {
            // Delete the page we found since we're creating a new one before it
            CatalogTupleDelete(lo_heap_r, &oldtuple->t_self);
        }

        // Create new page with zero-filled data up to truncation point
        char workbuf[LOBLKSIZE + VARHDRSZ];
        if (offset_in_page > 0)
            MemSet(VARDATA(workbuf), 0, offset_in_page);
        SET_VARSIZE(workbuf, offset_in_page + VARHDRSZ);

        // Insert new tuple
        Datum values[Natts_pg_largeobject] = {0};
        bool nulls[Natts_pg_largeobject] = {false};

        values[Anum_pg_largeobject_loid - 1] = ObjectIdGetDatum(obj_desc->id);
        values[Anum_pg_largeobject_pageno - 1] = Int32GetDatum(target_page);
        values[Anum_pg_largeobject_data - 1] = PointerGetDatum(workbuf);

        HeapTuple newtup = heap_form_tuple(lo_heap_r->rd_att, values, nulls);
        CatalogTupleInsertWithInfo(lo_heap_r, newtup, indstate);
        heap_freetuple(newtup);
    }

    // Delete all pages beyond the truncation point
    if (olddata != NULL) {
        while ((oldtuple = systable_getnext_ordered(sd, ForwardScanDirection)) != NULL) {
            CatalogTupleDelete(lo_heap_r, &oldtuple->t_self);
        }
    }

    // Cleanup
    systable_endscan_ordered(sd);
    CatalogCloseIndexes(indstate);
    CommandCounterIncrement();
}
```