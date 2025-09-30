# NextCopyFromRawFields

## Location
[src/backend/commands/copyfromparse.c:754-853](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfromparse.c#L754-L853)

## Overview
NextCopyFromRawFields reads raw field data from the next line in COPY FROM operations for text or CSV mode, returning all fields present in the input without applying force_not_null options.

## Definition

```c
bool
NextCopyFromRawFields(CopyFromState cstate, char ***fields, int *nfields)
```
## Detailed Description
This function is responsible for reading and parsing the next line of input data during COPY FROM operations in text or CSV format. It handles header line validation when required and parses the input line into individual field values. The function returns an internal temporary buffer containing all raw fields found in the input line, which remains valid until the next call to the function. Importantly, it does not apply force_not_null options to the returned fields, leaving them in their raw state for further processing.

The function performs header line validation if configured, ensuring that column names and counts match the expected relation schema. After validation (or if no header processing is needed), it reads the actual data line and parses it according to the specified format (CSV or text).

## Parameters / Member Variables
- : The COPY FROM state structure containing configuration, buffers, and parsing state
- : Output parameter that receives a pointer to the array of parsed field strings
- : Output parameter that receives the count of fields parsed from the input line

## Dependencies
- Functions called/Symbols referenced:
  - [CopyReadLine](../C/CopyReadLine.md): Reads the next line from input into the line buffer
  - [CopyReadAttributesCSV](../C/CopyReadAttributesCSV.md): Parses CSV-formatted line into individual fields
  - [CopyReadAttributesText](../C/CopyReadAttributesText.md): Parses text-formatted line into individual fields  
  - RelationGetDescr: Gets tuple descriptor for relation validation
  - lfirst_int: Extracts integer values from list cells
  - [namestrcmp](../n/namestrcmp.md): Compares PostgreSQL name structures
- Called from (representative examples):
  - [NextCopyFrom](NextCopyFrom.md): Higher-level function that processes COPY FROM operations

## Notes and Other Information
- Only available for text or CSV input modes (binary mode uses different parsing)
- The returned field array is internal and temporary - data becomes invalid on next call
- Header line processing includes validation of both field count and column name matching
- Returns false when EOF is encountered at the start of a line (no more data)
- The number of fields returned may differ from the number of relation columns
- Does not apply force_not_null transformations - fields remain in raw parsed state

## Simplified Source

```c
bool
NextCopyFromRawFields(CopyFromState cstate, char ***fields, int *nfields)
{
    int fldct;
    bool done;

    // Only for text/CSV mode (not binary)
    Assert(!cstate->opts.binary);

    // Handle header line validation on first line
    if (cstate->cur_lineno == 0 && cstate->opts.header_line) {
        TupleDesc tupDesc = RelationGetDescr(cstate->rel);
        cstate->cur_lineno++;
        done = CopyReadLine(cstate);

        // Validate header if matching is required
        if (cstate->opts.header_line == COPY_HEADER_MATCH) {
            // Parse header line
            if (cstate->opts.csv_mode)
                fldct = CopyReadAttributesCSV(cstate);
            else
                fldct = CopyReadAttributesText(cstate);

            // Check field count matches expected columns
            if (fldct != list_length(cstate->attnumlist))
                ereport(ERROR, (errmsg("wrong number of fields in header line")));

            // Validate each column name
            int fldnum = 0;
            ListCell *cur;
            foreach(cur, cstate->attnumlist) {
                int attnum = lfirst_int(cur);
                Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);
                char *colName = cstate->raw_fields[fldnum++];

                if (colName == NULL || namestrcmp(&attr->attname, colName) != 0)
                    ereport(ERROR, (errmsg("column name mismatch in header")));
            }
        }

        if (done)
            return false;
    }

    // Read next data line
    cstate->cur_lineno++;
    done = CopyReadLine(cstate);

    // EOF at start of line means we're done
    if (done && cstate->line_buf.len == 0)
        return false;

    // Parse line into fields based on format
    if (cstate->opts.csv_mode)
        fldct = CopyReadAttributesCSV(cstate);
    else
        fldct = CopyReadAttributesText(cstate);

    // Return parsed fields
    *fields = cstate->raw_fields;
    *nfields = fldct;
    return true;
}
```