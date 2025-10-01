# populate_record

## Location
[src/backend/utils/adt/jsonfuncs.c:3518-3633](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L3518-L3633)

## Overview
A static function that converts JSON/JSONB values into PostgreSQL record (composite type) tuples by populating field values according to the provided tuple descriptor.

## Definition

```c
structure */
		tuple.t_len = HeapTupleHeaderGetDatumLength(defaultval);
```
## Detailed Description
This function takes a JSON object and populates a PostgreSQL record tuple based on the structure defined by the tuple descriptor. It handles field mapping, type conversion, and maintains metadata caching for performance optimization. The function supports default values and proper handling of dropped columns and domain types.

Key behaviors:
- Returns the default value immediately if the JSON object is empty and a default is provided
- Allocates or reuses metadata cache for column information
- Invalidates cache when record type changes
- Processes each column by matching JSON field names to column names
- Handles dropped columns by setting them to NULL
- Ensures domain type validation even for missing fields

## Parameters / Member Variables
- : Tuple descriptor defining the target record structure
- : Pointer to cached metadata for record I/O operations (allocated/reused)
- : Optional default tuple header to use for missing values
- : Memory context for allocations
- : JSON object containing the field values to populate
- : Error context for reporting conversion errors

## Dependencies
- Functions called/Symbols referenced:
  - JsObjectIsEmpty
  - [allocate_record_info](../a/allocate_record_info.md)
  - MemSet
  - [heap_deform_tuple](../h/heap_deform_tuple.md)
  - [JsObjectGetField](../J/JsObjectGetField.md)
  - [populate_record_field](populate_record_field.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - HeapTupleHeaderGetDatumLength
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md)
- Called from (representative examples):
  - [populate_composite](populate_composite.md)
  - [populate_recordset_record](populate_recordset_record.md)

## Notes and Other Information
- This is a static function used internally by JSON processing functions
- Implements efficient metadata caching to avoid repeated type lookups
- Properly handles PostgreSQL's tuple structure including dropped columns
- Ensures domain type constraints are validated even for missing JSON fields
- Memory management is handled through the provided memory context
- Part of PostgreSQL's JSON/JSONB to record conversion infrastructure

## Simplified Source

```c
static HeapTupleHeader populate_record(TupleDesc tupdesc, RecordIOData **record_p,
                                      HeapTupleHeader defaultval, MemoryContext mcxt,
                                      JsObject *obj, Node *escontext)
{
    RecordIOData *record = *record_p;
    Datum *values;
    bool *nulls;
    HeapTuple res;
    int ncolumns = tupdesc->natts;
    int i;

    // Return default immediately if JSON object is empty and default provided
    if (defaultval && JsObjectIsEmpty(obj))
        return defaultval;

    // Allocate or reuse metadata cache
    if (record == NULL || record->ncolumns != ncolumns)
        *record_p = record = allocate_record_info(mcxt, ncolumns);

    // Invalidate cache if record type has changed
    if (record->record_type != tupdesc->tdtypeid ||
        record->record_typmod != tupdesc->tdtypmod)
    {
        MemSet(record, 0, offsetof(RecordIOData, columns) + ncolumns * sizeof(ColumnIOData));
        record->record_type = tupdesc->tdtypeid;
        record->record_typmod = tupdesc->tdtypmod;
        record->ncolumns = ncolumns;
    }

    // Allocate arrays for column values and null indicators
    values = (Datum *) palloc(ncolumns * sizeof(Datum));
    nulls = (bool *) palloc(ncolumns * sizeof(bool));

    // Initialize values from default tuple or set to null
    if (defaultval)
    {
        HeapTupleData tuple;
        tuple.t_len = HeapTupleHeaderGetDatumLength(defaultval);
        ItemPointerSetInvalid(&(tuple.t_self));
        tuple.t_tableOid = InvalidOid;
        tuple.t_data = defaultval;

        heap_deform_tuple(&tuple, tupdesc, values, nulls);
    }
    else
    {
        for (i = 0; i < ncolumns; ++i)
        {
            values[i] = (Datum) 0;
            nulls[i] = true;
        }
    }

    // Process each column, mapping JSON fields to record fields
    for (i = 0; i < ncolumns; ++i)
    {
        Form_pg_attribute att = TupleDescAttr(tupdesc, i);
        char *colname = NameStr(att->attname);
        JsValue field = {0};
        bool found;

        // Skip dropped columns
        if (att->attisdropped)
        {
            nulls[i] = true;
            continue;
        }

        found = JsObjectGetField(obj, colname, &field);

        // Skip if field not found and we have defaults
        if (defaultval && !found)
            continue;

        // Populate field value with type conversion
        values[i] = populate_record_field(&record->columns[i], att->atttypid, att->atttypmod,
                                         colname, mcxt, nulls[i] ? (Datum) 0 : values[i],
                                         &field, &nulls[i], escontext, false);
    }

    // Form the final tuple
    res = heap_form_tuple(tupdesc, values, nulls);

    pfree(values);
    pfree(nulls);

    return res->t_data;
}
```