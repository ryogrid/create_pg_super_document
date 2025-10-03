# toast_save_datum

## Location
[src/backend/access/common/toast_internals.c:119-384](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/toast_internals.c#L119-L384)

## Overview
Stores a large varlena datum into the secondary toast relation by splitting it into chunks, and returns a pointer datum that references the stored data.

## Definition

```c
Datum
toast_save_datum(Relation rel, Datum value,
				 struct varlena *oldexternal, int options)
```
## Detailed Description
This function handles the storage of large data values (TOASTed values) into PostgreSQL's secondary toast table. It takes a varlena datum that exceeds inline storage limits and splits it into fixed-size chunks stored as separate tuples in the associated toast relation. The function creates a toast pointer structure that contains metadata about the stored value, including its original size, compression information, and location identifiers.

The function handles several complex scenarios including table rewrites during operations like CLUSTER, where it preserves existing toast value OIDs to maintain referential integrity. It also optimizes for cases where the same toast value might be referenced multiple times during rewrite operations by detecting existing values and avoiding duplicate storage.

Data is stored in chunks up to TOAST_MAX_CHUNK_SIZE bytes each, with each chunk stored as a separate tuple containing the value ID, chunk sequence number, and chunk data. The function maintains proper indexing on the toast relation and ensures transactional consistency.

## Parameters / Member Variables
- `rel`: The main relation being worked with (not the toast relation itself)
- `value`: The datum containing the varlena data to be stored in toast storage
- `*oldexternal`: Optional pointer to previous external toast value (used during table rewrites for OID preservation)
- `options`: Options passed to heap_insert() when storing toast row tuples
## Dependencies
- Functions called/Symbols referenced:
  - [table_open](table_open.md)
  - [toast_open_indexes](toast_open_indexes.md)
  - [toast_close_indexes](toast_close_indexes.md)
  - [GetCurrentCommandId](../G/GetCurrentCommandId.md)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - [toastrel_valueid_exists](toastrel_valueid_exists.md)
  - [toastid_valueid_exists](toastid_valueid_exists.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [heap_insert](../h/heap_insert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [index_insert](../i/index_insert.md)
  - [palloc](../p/palloc.md)
  - memcpy
  - VARATT_EXTERNAL_GET_POINTER
  - SET_VARTAG_EXTERNAL
- Called from (representative examples):
  - [toast_tuple_externalize](toast_tuple_externalize.md)

## Notes and Other Information
- Splits large values into chunks of maximum TOAST_MAX_CHUNK_SIZE bytes each
- Handles different varlena formats: short headers, compressed data, and regular format
- During table rewrite operations, attempts to preserve existing toast value OIDs when possible
- Creates proper index entries for all ready indexes on the toast relation
- Maintains transactional locks on toast relation until commit to prevent concurrent reindex conflicts
- The returned toast pointer contains va_toastrelid, va_valueid, va_rawsize, and va_extinfo fields
- Optimizes storage by detecting and avoiding duplicate toast values during rewrites

## Simplified Source

```c
Datum toast_save_datum(Relation rel, Datum value,
                      struct varlena *oldexternal, int options) {
    Relation toastrel;
    Relation *toastidxs;
    struct varatt_external toast_pointer;
    char *data_p;
    int32 data_todo, chunk_size, chunk_seq = 0;
    int num_indexes, validIndex;

    // Open toast relation and indexes
    toastrel = table_open(rel->rd_rel->reltoastrelid, RowExclusiveLock);
    validIndex = toast_open_indexes(toastrel, RowExclusiveLock,
                                   &toastidxs, &num_indexes);

    // Extract data pointer and size based on varlena format
    if (VARATT_IS_SHORT(value)) {
        data_p = VARDATA_SHORT(value);
        data_todo = VARSIZE_SHORT(value) - VARHDRSZ_SHORT;
        toast_pointer.va_rawsize = data_todo + VARHDRSZ;
    } else if (VARATT_IS_COMPRESSED(value)) {
        data_p = VARDATA(value);
        data_todo = VARSIZE(value) - VARHDRSZ;
        toast_pointer.va_rawsize = VARDATA_COMPRESSED_GET_EXTSIZE(value) + VARHDRSZ;
        // Set compression info in external size field
        VARATT_EXTERNAL_SET_SIZE_AND_COMPRESS_METHOD(toast_pointer, data_todo,
            VARDATA_COMPRESSED_GET_COMPRESS_METHOD(value));
    } else {
        data_p = VARDATA(value);
        data_todo = VARSIZE(value) - VARHDRSZ;
        toast_pointer.va_rawsize = VARSIZE(value);
    }

    // Set toast relation ID
    toast_pointer.va_toastrelid = OidIsValid(rel->rd_toastoid) ?
                                 rel->rd_toastoid : RelationGetRelid(toastrel);

    // Choose value ID (OID for this toast value)
    if (!OidIsValid(rel->rd_toastoid)) {
        // Normal case: get new unique OID
        toast_pointer.va_valueid = GetNewOidWithIndex(toastrel,
            RelationGetRelid(toastidxs[validIndex]), 1);
    } else {
        // Table rewrite case: try to preserve old OID if possible
        toast_pointer.va_valueid = InvalidOid;
        if (oldexternal && old_value_from_same_toast_table) {
            toast_pointer.va_valueid = old_toast_pointer.va_valueid;
            // Check if already exists to avoid duplicates
            if (toastrel_valueid_exists(toastrel, toast_pointer.va_valueid)) {
                data_todo = 0; // Skip storage, value already exists
            }
        }
        if (toast_pointer.va_valueid == InvalidOid) {
            // Get new OID that doesn't conflict with old or new toast table
            do {
                toast_pointer.va_valueid = GetNewOidWithIndex(toastrel,
                    RelationGetRelid(toastidxs[validIndex]), 1);
            } while (toastid_valueid_exists(rel->rd_toastoid,
                                          toast_pointer.va_valueid));
        }
    }

    // Store data in chunks
    while (data_todo > 0) {
        // Calculate chunk size (up to TOAST_MAX_CHUNK_SIZE)
        chunk_size = Min(TOAST_MAX_CHUNK_SIZE, data_todo);

        // Create and insert chunk tuple
        HeapTuple chunk_tuple = create_chunk_tuple(toast_pointer.va_valueid,
                                                  chunk_seq++, data_p, chunk_size);
        heap_insert(toastrel, chunk_tuple, GetCurrentCommandId(true), options, NULL);

        // Update all ready indexes
        for (int i = 0; i < num_indexes; i++) {
            if (toastidxs[i]->rd_index->indisready) {
                index_insert(toastidxs[i], chunk_values, chunk_nulls,
                           &(chunk_tuple->t_self), toastrel, UNIQUE_CHECK_YES,
                           false, NULL);
            }
        }

        heap_freetuple(chunk_tuple);

        // Move to next chunk
        data_todo -= chunk_size;
        data_p += chunk_size;
    }

    // Close relations
    toast_close_indexes(toastidxs, num_indexes, NoLock);
    table_close(toastrel, NoLock);

    // Create and return toast pointer
    struct varlena *result = (struct varlena *) palloc(TOAST_POINTER_SIZE);
    SET_VARTAG_EXTERNAL(result, VARTAG_ONDISK);
    memcpy(VARDATA_EXTERNAL(result), &toast_pointer, sizeof(toast_pointer));

    return PointerGetDatum(result);
}
```