# record_out

## Location
[src/backend/utils/adt/rowtypes.c:329-479](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rowtypes.c#L329-L479)

## Overview
Converts the internal binary representation of a composite type (record) into its string representation for PostgreSQL output.

## Definition

```c
structure */
	tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
```
## Detailed Description
The  function serves as the output conversion function for any composite type in PostgreSQL. It takes a  (the internal binary format) and converts it to a human-readable string representation in the format . The function handles proper quoting of values that contain special characters, escape sequence generation, and null value representation.

The function extracts type information directly from the tuple header, decomposes the tuple into individual column values, and formats each value using the appropriate type-specific output function. It implements intelligent quoting logic to only add quotes when necessary and properly escapes quotes and backslashes within values.

## Parameters / Member Variables
- : Input  containing the binary representation of the record to be converted

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md): Stack overflow protection for recursive calls
  - HeapTupleHeaderGetTypeId: Extracts type OID from tuple header
  - HeapTupleHeaderGetTypMod: Extracts type modifier from tuple header
  - [lookup_rowtype_tupdesc](../l/lookup_rowtype_tupdesc.md): Retrieves tuple descriptor for the record type
  - [heap_deform_tuple](../h/heap_deform_tuple.md): Extracts individual column values from tuple
  - [getTypeOutputInfo](../g/getTypeOutputInfo.md): Gets output function info for column types
  - [OutputFunctionCall](../O/OutputFunctionCall.md): Calls type-specific output functions
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md): Memory allocation in function context
  - ReleaseTupleDesc: Releases tuple descriptor reference

- Called from (representative examples):
  - Type system as registered output function for composite types
  - SQL result formatting when displaying record values

## Notes and Other Information
- Implements smart quoting: only quotes values containing special characters (quotes, backslashes, parentheses, commas, or whitespace)
- Properly escapes quotes and backslashes by doubling them within quoted values
- Forces quotes for empty strings to distinguish from null values
- Handles dropped columns by skipping them in output
- Uses function-local caching (fn_extra) to optimize repeated calls with same type
- Memory management ensures result string is properly allocated for caller

## Simplified Source

```c
Datum
record_out(PG_FUNCTION_ARGS)
{
    HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(0);
    Oid tupType;
    int32 tupTypmod;
    TupleDesc tupdesc;
    HeapTupleData tuple;
    RecordIOData *my_extra;
    bool needComma = false;
    int ncolumns, i;
    Datum *values;
    bool *nulls;
    StringInfoData buf;

    check_stack_depth();

    // Extract type info from tuple header
    tupType = HeapTupleHeaderGetTypeId(rec);
    tupTypmod = HeapTupleHeaderGetTypMod(rec);
    tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
    ncolumns = tupdesc->natts;

    // Build temporary tuple control structure
    tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
    ItemPointerSetInvalid(&(tuple.t_self));
    tuple.t_tableOid = InvalidOid;
    tuple.t_data = rec;

    // Set up or validate cached I/O info
    my_extra = (RecordIOData *) fcinfo->flinfo->fn_extra;
    if (my_extra == NULL || my_extra->ncolumns != ncolumns) {
        fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
                                                      offsetof(RecordIOData, columns) +
                                                      ncolumns * sizeof(ColumnIOData));
        my_extra = (RecordIOData *) fcinfo->flinfo->fn_extra;
        my_extra->record_type = InvalidOid;
        my_extra->record_typmod = 0;
    }

    if (my_extra->record_type != tupType || my_extra->record_typmod != tupTypmod) {
        MemSet(my_extra, 0, offsetof(RecordIOData, columns) + ncolumns * sizeof(ColumnIOData));
        my_extra->record_type = tupType;
        my_extra->record_typmod = tupTypmod;
        my_extra->ncolumns = ncolumns;
    }

    values = (Datum *) palloc(ncolumns * sizeof(Datum));
    nulls = (bool *) palloc(ncolumns * sizeof(bool));

    // Extract individual field values
    heap_deform_tuple(&tuple, tupdesc, values, nulls);

    // Build output string
    initStringInfo(&buf);
    appendStringInfoChar(&buf, '(');

    for (i = 0; i < ncolumns; i++) {
        Form_pg_attribute att = TupleDescAttr(tupdesc, i);
        ColumnIOData *column_info = &my_extra->columns[i];
        Oid column_type = att->atttypid;
        Datum attr;
        char *value;
        char *tmp;
        bool nq;

        // Skip dropped columns
        if (att->attisdropped)
            continue;

        if (needComma)
            appendStringInfoChar(&buf, ',');
        needComma = true;

        if (nulls[i]) {
            // Null fields appear as empty
            continue;
        }

        // Convert field to text
        if (column_info->column_type != column_type) {
            getTypeOutputInfo(column_type, &column_info->typiofunc, &column_info->typisvarlena);
            fmgr_info_cxt(column_info->typiofunc, &column_info->proc, fcinfo->flinfo->fn_mcxt);
            column_info->column_type = column_type;
        }

        attr = values[i];
        value = OutputFunctionCall(&column_info->proc, attr);

        // Determine if quoting is needed
        nq = (value[0] == '\0');  // Force quotes for empty string
        for (tmp = value; *tmp; tmp++) {
            char ch = *tmp;
            if (ch == '"' || ch == '\\' || ch == '(' || ch == ')' || ch == ',' ||
                isspace((unsigned char) ch)) {
                nq = true;
                break;
            }
        }

        // Output the value with proper quoting/escaping
        if (nq)
            appendStringInfoCharMacro(&buf, '"');
        for (tmp = value; *tmp; tmp++) {
            char ch = *tmp;
            if (ch == '"' || ch == '\\')
                appendStringInfoCharMacro(&buf, ch);  // Double the character
            appendStringInfoCharMacro(&buf, ch);
        }
        if (nq)
            appendStringInfoCharMacro(&buf, '"');
    }

    appendStringInfoChar(&buf, ')');

    pfree(values);
    pfree(nulls);
    ReleaseTupleDesc(tupdesc);

    PG_RETURN_CSTRING(buf.data);
}
```