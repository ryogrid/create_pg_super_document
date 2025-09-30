# NextCopyFrom

## Location
[src/backend/commands/copyfromparse.c:854-1098](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfromparse.c#L854-L1098)

## Overview
NextCopyFrom reads and processes the next complete tuple from a COPY FROM operation, handling both text/CSV and binary formats while applying type conversions, defaults, and error handling.

## Definition

```c
bool
NextCopyFrom(CopyFromState cstate, ExprContext *econtext,
			 Datum *values, bool *nulls)
```
## Detailed Description
This function is the main entry point for reading and processing individual tuples during COPY FROM operations. It handles both text/CSV and binary input formats, performing complete tuple construction including type conversion, default value evaluation, and error handling. For text/CSV mode, it calls NextCopyFromRawFields to get raw field data, then processes each field through input functions and applies FORCE_NULL/FORCE_NOT_NULL options. For binary mode, it reads the field count and binary data directly. The function also handles default expressions for columns not present in the input data and implements error recovery when ON_ERROR IGNORE is specified.

The function initializes all output arrays to NULL/true, then populates them based on the input data format. It ensures proper type conversion using the relation's input functions and handles various COPY options like null handling and default expressions. Error handling includes soft error recovery and detailed logging when configured.

## Parameters / Member Variables
- : The COPY FROM state structure containing configuration, input functions, and parsing state
- : Expression context for evaluating default expressions; can be NULL if no defaults are used
- : Output array of Datum values, one per relation column, filled by this function
- : Output array of null indicators, one per relation column, filled by this function

## Dependencies
- Functions called/Symbols referenced:
  - [NextCopyFromRawFields](NextCopyFromRawFields.md): Reads raw field strings from input for text/CSV mode
  - MemSet: Initializes arrays to default values
  - [InputFunctionCallSafe](../I/InputFunctionCallSafe.md): Safely converts string input to typed Datum values
  - [ExecEvalExpr](../E/ExecEvalExpr.md): Evaluates default expressions for missing columns
  - [CopyGetInt16](../C/CopyGetInt16.md): Reads 16-bit integers from binary input
  - [CopyReadBinaryData](../C/CopyReadBinaryData.md): Reads raw binary data from input stream
  - [CopyReadBinaryAttribute](../C/CopyReadBinaryAttribute.md): Reads and converts binary attribute data
  - CopyLimitPrintoutLength: Limits output length for error messages
  - lfirst_int: Extracts integer values from list cells
- Called from (representative examples):
  - [CopyFrom](../C/CopyFrom.md): Main COPY FROM processing loop

## Notes and Other Information
- Supports both text/CSV and binary input formats with format-specific processing paths
- Handles FORCE_NULL and FORCE_NOT_NULL options in CSV mode for flexible null handling
- Implements soft error recovery when ON_ERROR IGNORE is specified, allowing processing to continue
- Requires proper memory context setup when default expressions are used (per-tuple context)
- Returns false when no more tuples are available (EOF or end of data)
- Maintains current line number and attribute name for error reporting context
- All output arrays must be pre-allocated to match the relation's column count
- Default expressions are evaluated after input processing for columns not in the input data

## Simplified Source
```c
bool NextCopyFrom(CopyFromState cstate, ExprContext *econtext,
                  Datum *values, bool *nulls) {
    TupleDesc tupDesc = RelationGetDescr(cstate->rel);
    AttrNumber num_phys_attrs = tupDesc->natts;
    AttrNumber attr_count = list_length(cstate->attnumlist);
    FmgrInfo *in_functions = cstate->in_functions;
    Oid *typioparams = cstate->typioparams;

    // Initialize all values to NULL
    MemSet(values, 0, num_phys_attrs * sizeof(Datum));
    MemSet(nulls, true, num_phys_attrs * sizeof(bool));
    MemSet(cstate->defaults, false, num_phys_attrs * sizeof(bool));

    if (!cstate->opts.binary) {
        // Text/CSV format processing
        char **field_strings;
        int fldct, fieldno = 0;

        // Read raw fields from input
        if (!NextCopyFromRawFields(cstate, &field_strings, &fldct))
            return false;

        // Check for extra fields
        if (attr_count > 0 && fldct > attr_count)
            ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
                           errmsg("extra data after last expected column")));

        // Process each input field
        ListCell *cur;
        foreach(cur, cstate->attnumlist) {
            int attnum = lfirst_int(cur);
            int m = attnum - 1;
            Form_pg_attribute att = TupleDescAttr(tupDesc, m);
            char *string;

            if (fieldno >= fldct)
                ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
                               errmsg("missing data for column \"%s\"", NameStr(att->attname))));

            string = field_strings[fieldno++];

            // Skip if column not being converted
            if (cstate->convert_select_flags && !cstate->convert_select_flags[m])
                continue;

            // Handle CSV FORCE_NULL/FORCE_NOT_NULL options
            if (cstate->opts.csv_mode) {
                if (string == NULL && cstate->opts.force_notnull_flags[m])
                    string = cstate->opts.null_print;
                else if (string != NULL && cstate->opts.force_null_flags[m] &&
                         strcmp(string, cstate->opts.null_print) == 0)
                    string = NULL;
            }

            cstate->cur_attname = NameStr(att->attname);
            cstate->cur_attval = string;

            if (string != NULL)
                nulls[m] = false;

            // Handle default expressions or input function conversion
            if (cstate->defaults[m]) {
                values[m] = ExecEvalExpr(cstate->defexprs[m], econtext, &nulls[m]);
            } else if (!InputFunctionCallSafe(&in_functions[m], string, typioparams[m],
                                             att->atttypmod, (Node *) cstate->escontext, &values[m])) {
                // Handle conversion errors with ON_ERROR IGNORE
                cstate->num_errors++;
                if (cstate->opts.log_verbosity == COPY_LOG_VERBOSITY_VERBOSE) {
                    // Log detailed error information
                    cstate->relname_only = true;
                    ereport(NOTICE, (errmsg("skipping row due to data type incompatibility at line %llu for column \"%s\"",
                                           (unsigned long long) cstate->cur_lineno, cstate->cur_attname)));
                    cstate->relname_only = false;
                }
                return true; // Skip this row
            }

            cstate->cur_attname = NULL;
            cstate->cur_attval = NULL;
        }
    } else {
        // Binary format processing
        int16 fld_count;
        cstate->cur_lineno++;

        // Read field count
        if (!CopyGetInt16(cstate, &fld_count))
            return false; // EOF

        if (fld_count == -1) {
            // EOF marker - ensure no trailing data
            char dummy;
            if (CopyReadBinaryData(cstate, &dummy, 1) > 0)
                ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
                               errmsg("received copy data after EOF marker")));
            return false;
        }

        if (fld_count != attr_count)
            ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
                           errmsg("row field count is %d, expected %d", (int) fld_count, attr_count)));

        // Read binary attributes
        ListCell *cur;
        foreach(cur, cstate->attnumlist) {
            int attnum = lfirst_int(cur);
            int m = attnum - 1;
            Form_pg_attribute att = TupleDescAttr(tupDesc, m);

            cstate->cur_attname = NameStr(att->attname);
            values[m] = CopyReadBinaryAttribute(cstate, &in_functions[m], typioparams[m],
                                               att->atttypmod, &nulls[m]);
            cstate->cur_attname = NULL;
        }
    }

    // Evaluate default expressions for missing columns
    for (int i = 0; i < cstate->num_defaults; i++) {
        values[cstate->defmap[i]] = ExecEvalExpr(cstate->defexprs[cstate->defmap[i]],
                                                econtext, &nulls[cstate->defmap[i]]);
    }

    return true;
}
```