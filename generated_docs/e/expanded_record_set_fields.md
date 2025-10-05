# expanded_record_set_fields

## Location
[src/backend/utils/adt/expandedrecord.c:1249-1378](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/expandedrecord.c#L1249-L1378)

## Overview
Sets all fields of an expanded record in one operation, ensuring proper memory management and type consistency for PostgreSQL record data structures.

## Definition

```c
structed the tuple, do that */
	if (!(erh->flags & ER_FLAG_DVALUES_VALID))
		deconstruct_expanded_record(erh);
```
## Detailed Description
This function performs bulk assignment of field values to an expanded record, providing an efficient way to initialize or completely replace all fields at once. Unlike individual field assignments via , this function does not guarantee atomicity or corruption-free state in case of errors, making it primarily suitable for initializing new expanded records.

The function handles proper memory management by copying non-by-value fields into the record's memory context, optionally detoasting external TOAST values based on the  parameter. It maintains the expanded record's internal flags to track data validity and external dependencies.

## Parameters / Member Variables
- : Pointer to the ExpandedRecordHeader structure to modify
- : Array of Datum values to assign to record fields
- : Array of boolean flags indicating which fields are NULL
- : Boolean flag controlling whether to forcibly detoast external TOAST values

## Dependencies
- Functions called/Symbols referenced:
  - [deconstruct_expanded_record](../d/deconstruct_expanded_record.md)
  - VARATT_IS_EXTERNAL
  - [detoast_external_attr](../d/detoast_external_attr.md)
  - [datumCopy](../d/datumCopy.md)
  - [get_short_term_cxt](../g/get_short_term_cxt.md)
  - [domain_check](../d/domain_check.md)
  - [ExpandedRecordGetRODatum](../E/ExpandedRecordGetRODatum.md)
- Called from (representative examples):
  - No direct callers found in codebase

## Notes and Other Information
- Function assumes caller has verified that provided datums match the record's rowtype
- Does not guarantee atomicity - errors may leave record in corrupted state
- Primarily intended for initializing new expanded records rather than updating existing ones
- Automatically handles domain constraint checking if the record represents a domain type
- Sets ER_FLAG_DVALUES_ALLOCED when allocating memory for non-by-value fields
- Invalidates flattened representation (ER_FLAG_FVALUE_VALID) since fields have changed

## Simplified Source

```c
void expanded_record_set_fields(ExpandedRecordHeader *erh,
                               const Datum *newValues, const bool *isnulls,
                               bool expand_external)
{
    // Ensure record is deconstructed
    if (!(erh->flags & ER_FLAG_DVALUES_VALID))
        deconstruct_expanded_record(erh);

    TupleDesc tupdesc = erh->er_tupdesc;

    // Invalidate flattened representation
    erh->flags &= ~ER_FLAG_FVALUE_VALID;
    erh->flat_size = 0;

    MemoryContext oldcxt = MemoryContextSwitchTo(erh->hdr.eoh_context);

    // Process each field
    for (int fnumber = 0; fnumber < erh->nfields; fnumber++) {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, fnumber);

        if (attr->attisdropped)
            continue;

        Datum newValue = newValues[fnumber];
        bool isnull = isnulls[fnumber];

        // Handle non-byval values
        if (!attr->attbyval && !isnull) {
            // Handle external values
            if (attr->attlen == -1 && VARATT_IS_EXTERNAL(DatumGetPointer(newValue))) {
                if (expand_external) {
                    newValue = PointerGetDatum(detoast_external_attr((struct varlena *) DatumGetPointer(newValue)));
                } else {
                    newValue = datumCopy(newValue, false, -1);
                    if (VARATT_IS_EXTERNAL(DatumGetPointer(newValue)))
                        erh->flags |= ER_FLAG_HAVE_EXTERNAL;
                }
            } else {
                newValue = datumCopy(newValue, false, attr->attlen);
            }
            erh->flags |= ER_FLAG_DVALUES_ALLOCED;

            // Free old value if present
            if (!erh->dnulls[fnumber]) {
                char *oldValue = (char *) DatumGetPointer(erh->dvalues[fnumber]);
                if (oldValue < erh->fstartptr || oldValue >= erh->fendptr)
                    pfree(oldValue);
            }
        }

        // Set new field value
        erh->dvalues[fnumber] = newValue;
        erh->dnulls[fnumber] = isnull;
    }

    // Check domain constraints if needed
    if (erh->flags & ER_FLAG_IS_DOMAIN) {
        MemoryContextSwitchTo(get_short_term_cxt(erh));
        domain_check(ExpandedRecordGetRODatum(erh), false,
                    erh->er_decltypeid, &erh->er_domaininfo,
                    erh->hdr.eoh_context);
    }

    MemoryContextSwitchTo(oldcxt);
}
```