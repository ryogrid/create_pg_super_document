# record_in

## Location
[src/backend/utils/adt/rowtypes.c:74-328](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rowtypes.c#L74-L328)

## Overview
Converts a string representation of a composite type (record) into its internal binary format for PostgreSQL storage.

## Definition

```c
structure */
	tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
```
## Detailed Description
The  function serves as the input conversion function for any composite type in PostgreSQL. It parses string representations of records in the format  and converts them into the internal  format used by PostgreSQL for storage and manipulation. The function handles complex parsing requirements including quote handling, escape sequences, null values, and nested composite types through recursive calls.

The function performs comprehensive validation of the input string format, ensures proper column count matching, and converts each field value using the appropriate type-specific input function. It maintains performance by caching I/O information for repeated calls with the same record type.

## Parameters / Member Variables
- : Input string representation of the record in format 
- : OID identifying the composite type being parsed
- : Type modifier for the composite type (-1 for standard composite types)
- : Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md): Stack overflow protection for recursive calls
  - [lookup_rowtype_tupdesc](../l/lookup_rowtype_tupdesc.md): Retrieves tuple descriptor for the record type
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md): Memory allocation in function context
  - [getTypeInputInfo](../g/getTypeInputInfo.md): Gets input function info for column types
  - [InputFunctionCallSafe](../I/InputFunctionCallSafe.md): Safely calls type-specific input functions
  - [heap_form_tuple](../h/heap_form_tuple.md): Creates heap tuple from values array
  - ReleaseTupleDesc: Releases tuple descriptor reference

- Called from (representative examples):
  - Type system as registered input function for composite types
  - SQL parsing when processing record literals

## Notes and Other Information
- Supports both quoted and unquoted field values with proper escape sequence handling
- Handles anonymous record types (RECORD) only when valid typmod is provided
- Uses function-local caching (fn_extra) to optimize repeated calls with same type
- Implements comprehensive error reporting for malformed input strings
- Supports soft error handling through error context parameter
- Memory management ensures result can be safely freed by caller

## Simplified Source

```c
Datum
record_in(PG_FUNCTION_ARGS)
{
    char *string = PG_GETARG_CSTRING(0);
    Oid tupType = PG_GETARG_OID(1);
    int32 tupTypmod = PG_GETARG_INT32(2);
    Node *escontext = fcinfo->context;
    HeapTupleHeader result;
    TupleDesc tupdesc;
    HeapTuple tuple;
    RecordIOData *my_extra;
    bool needComma = false;
    int ncolumns, i;
    char *ptr;
    Datum *values;
    bool *nulls;
    StringInfoData buf;

    check_stack_depth();

    // Validate that we have enough info to identify the record type
    if (tupType == RECORDOID && tupTypmod < 0)
        ereturn(escontext, (Datum) 0,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("input of anonymous composite types is not implemented")));

    // Get tuple descriptor for this record type
    tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
    ncolumns = tupdesc->natts;

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

    // Parse the input string: expect format "(field1,field2,...)"
    ptr = string;
    while (*ptr && isspace((unsigned char) *ptr)) ptr++;  // Skip whitespace
    if (*ptr++ != '(') {
        errsave(escontext, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                           errmsg("malformed record literal: \"%s\"", string),
                           errdetail("Missing left parenthesis.")));
        goto fail;
    }

    initStringInfo(&buf);

    // Parse each field
    for (i = 0; i < ncolumns; i++) {
        Form_pg_attribute att = TupleDescAttr(tupdesc, i);
        ColumnIOData *column_info = &my_extra->columns[i];
        Oid column_type = att->atttypid;
        char *column_data;

        // Skip dropped columns
        if (att->attisdropped) {
            values[i] = (Datum) 0;
            nulls[i] = true;
            continue;
        }

        // Handle comma separator
        if (needComma) {
            if (*ptr == ',')
                ptr++;
            else {
                errsave(escontext, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                                   errmsg("malformed record literal: \"%s\"", string),
                                   errdetail("Too few columns.")));
                goto fail;
            }
        }

        // Check for null value (empty field)
        if (*ptr == ',' || *ptr == ')') {
            column_data = NULL;
            nulls[i] = true;
        } else {
            // Extract field value with quote handling
            bool inquote = false;
            resetStringInfo(&buf);

            while (inquote || !(*ptr == ',' || *ptr == ')')) {
                char ch = *ptr++;
                if (ch == '\0') {
                    errsave(escontext, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                                       errmsg("malformed record literal: \"%s\"", string),
                                       errdetail("Unexpected end of input.")));
                    goto fail;
                }
                if (ch == '\\') {
                    if (*ptr == '\0') {
                        errsave(escontext, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                                           errmsg("malformed record literal: \"%s\"", string),
                                           errdetail("Unexpected end of input.")));
                        goto fail;
                    }
                    appendStringInfoChar(&buf, *ptr++);
                } else if (ch == '"') {
                    if (!inquote)
                        inquote = true;
                    else if (*ptr == '"') {
                        appendStringInfoChar(&buf, *ptr++);  // Escaped quote
                    } else
                        inquote = false;
                } else
                    appendStringInfoChar(&buf, ch);
            }
            column_data = buf.data;
            nulls[i] = false;
        }

        // Convert the field value using appropriate input function
        if (column_info->column_type != column_type) {
            getTypeInputInfo(column_type, &column_info->typiofunc, &column_info->typioparam);
            fmgr_info_cxt(column_info->typiofunc, &column_info->proc, fcinfo->flinfo->fn_mcxt);
            column_info->column_type = column_type;
        }

        if (!InputFunctionCallSafe(&column_info->proc, column_data, column_info->typioparam,
                                  att->atttypmod, escontext, &values[i]))
            goto fail;

        needComma = true;
    }

    // Verify closing parenthesis
    if (*ptr++ != ')') {
        errsave(escontext, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                           errmsg("malformed record literal: \"%s\"", string),
                           errdetail("Too many columns.")));
        goto fail;
    }

    // Check for trailing junk
    while (*ptr && isspace((unsigned char) *ptr)) ptr++;
    if (*ptr) {
        errsave(escontext, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                           errmsg("malformed record literal: \"%s\"", string),
                           errdetail("Junk after right parenthesis.")));
        goto fail;
    }

    // Create the result tuple
    tuple = heap_form_tuple(tupdesc, values, nulls);
    result = (HeapTupleHeader) palloc(tuple->t_len);
    memcpy(result, tuple->t_data, tuple->t_len);

    // Cleanup
    heap_freetuple(tuple);
    pfree(buf.data);
    pfree(values);
    pfree(nulls);
    ReleaseTupleDesc(tupdesc);

    PG_RETURN_HEAPTUPLEHEADER(result);

fail:
    ReleaseTupleDesc(tupdesc);
    PG_RETURN_NULL();
}
```