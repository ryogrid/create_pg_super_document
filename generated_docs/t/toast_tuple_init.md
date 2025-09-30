# toast_tuple_init

## Location
[src/backend/access/table/toast_helper.c:41-180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/table/toast_helper.c#L41-L180)

## Overview
Initializes a TOAST tuple context structure to prepare for toasting operations on a tuple, setting up attribute flags and handling external values from existing tuples during updates.

## Definition

```c
struct varlena *old_value;
```
## Detailed Description
This function prepares the ToastTupleContext structure for tuple toasting operations. It analyzes each attribute in the tuple to determine what toasting actions are needed. For new tuples (INSERT), it simply examines the new values. For updates (UPDATE), it compares old and new values to determine which external values need cleanup and which can be reused.

The function iterates through all attributes in the tuple descriptor and:
- Initializes per-attribute flags and metadata
- For updates, compares old and new external values to determine if cleanup is needed
- Handles NULL attributes appropriately
- Processes varlena attributes and sets up proper storage strategy
- Fetches external values that cannot be reused
- Sets various flags to indicate what operations will be needed during toasting

## Parameters / Member Variables
- : ToastTupleContext structure containing:
  - : Relation descriptor
  - : Array of new attribute values
  - : Array of NULL flags for new values
  - : Array of old attribute values (NULL for INSERT)
  - : Array of NULL flags for old values (NULL for INSERT)
  - : Output array of per-attribute toast information
  - : Output flags indicating needed operations

## Dependencies
- Functions called/Symbols referenced:
  - TupleDescAttr
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - VARATT_IS_EXTERNAL_ONDISK
  - VARSIZE_EXTERNAL
  - VARATT_IS_EXTERNAL
  - [detoast_attr](../d/detoast_attr.md)
  - [detoast_external_attr](../d/detoast_external_attr.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - VARSIZE_ANY
- Called from (representative examples):
  - [heap_toast_insert_or_update](../h/heap_toast_insert_or_update.md)

## Notes and Other Information
- This is the first step in the tuple toasting process, setting up the context for subsequent compression and externalization operations
- The function carefully handles UPDATE scenarios by comparing old and new values to avoid unnecessary work
- External values that haven't changed can be reused, avoiding the need to re-externalize them
- Sets up flags that guide later stages of the toasting process
- Part of PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) system for handling large attribute values

## Simplified Source

```c
void toast_tuple_init(ToastTupleContext *ttc) {
    TupleDesc tupleDesc = ttc->ttc_rel->rd_att;
    int numAttrs = tupleDesc->natts;
    int i;

    ttc->ttc_flags = 0;

    for (i = 0; i < numAttrs; i++) {
        Form_pg_attribute att = TupleDescAttr(tupleDesc, i);
        struct varlena *old_value;
        struct varlena *new_value;

        // Initialize attribute metadata
        ttc->ttc_attr[i].tai_colflags = 0;
        ttc->ttc_attr[i].tai_oldexternal = NULL;
        ttc->ttc_attr[i].tai_compression = att->attcompression;

        if (ttc->ttc_oldvalues != NULL) {
            // Handle UPDATE case - compare old and new values
            old_value = (struct varlena *) DatumGetPointer(ttc->ttc_oldvalues[i]);
            new_value = (struct varlena *) DatumGetPointer(ttc->ttc_values[i]);

            // Check if old external value needs deletion
            if (att->attlen == -1 && !ttc->ttc_oldisnull[i] &&
                VARATT_IS_EXTERNAL_ONDISK(old_value)) {

                if (ttc->ttc_isnull[i] ||
                    !VARATT_IS_EXTERNAL_ONDISK(new_value) ||
                    memcmp((char *) old_value, (char *) new_value,
                           VARSIZE_EXTERNAL(old_value)) != 0) {
                    // Old value is no longer needed
                    ttc->ttc_attr[i].tai_colflags |= TOASTCOL_NEEDS_DELETE_OLD;
                    ttc->ttc_flags |= TOAST_NEEDS_DELETE_OLD;
                } else {
                    // Reuse unchanged external value
                    ttc->ttc_attr[i].tai_colflags |= TOASTCOL_IGNORE;
                    continue;
                }
            }
        } else {
            // Handle INSERT case - just get new value
            new_value = (struct varlena *) DatumGetPointer(ttc->ttc_values[i]);
        }

        // Handle NULL attributes
        if (ttc->ttc_isnull[i]) {
            ttc->ttc_attr[i].tai_colflags |= TOASTCOL_IGNORE;
            ttc->ttc_flags |= TOAST_HAS_NULLS;
            continue;
        }

        // Process varlena attributes
        if (att->attlen == -1) {
            if (att->attstorage == TYPSTORAGE_PLAIN)
                ttc->ttc_attr[i].tai_colflags |= TOASTCOL_IGNORE;

            // Handle external values that need to be fetched
            if (VARATT_IS_EXTERNAL(new_value)) {
                ttc->ttc_attr[i].tai_oldexternal = new_value;
                if (att->attstorage == TYPSTORAGE_PLAIN)
                    new_value = detoast_attr(new_value);
                else
                    new_value = detoast_external_attr(new_value);
                ttc->ttc_values[i] = PointerGetDatum(new_value);
                ttc->ttc_attr[i].tai_colflags |= TOASTCOL_NEEDS_FREE;
                ttc->ttc_flags |= (TOAST_NEEDS_CHANGE | TOAST_NEEDS_FREE);
            }

            // Record attribute size
            ttc->ttc_attr[i].tai_size = VARSIZE_ANY(new_value);
        } else {
            // Fixed-length attribute - no toasting needed
            ttc->ttc_attr[i].tai_colflags |= TOASTCOL_IGNORE;
        }
    }
}
```