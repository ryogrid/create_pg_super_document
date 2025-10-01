# CopyReadAttributesCSV

## Location
[src/backend/commands/copyfromparse.c:1791-1985](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfromparse.c#L1791-L1985)

## Overview
Parses a single line of CSV-format COPY data into separate attribute fields, handling CSV-specific features like quoted fields, escape sequences, and delimiter handling according to standard CSV conventions.

## Definition

```c
static int
CopyReadAttributesCSV(CopyFromState cstate)
```
## Detailed Description
This function serves as the CSV counterpart to , implementing RFC 4180-compliant CSV parsing for PostgreSQL's COPY operations. The parser handles the complexities of CSV format including quoted fields that can contain delimiters and newlines, escape sequences within quoted contexts, and proper handling of quote characters themselves.

The function implements a state machine approach with two primary modes:
1. **"Not in quote" mode**: Normal field parsing where delimiters separate fields
2. **"In quote" mode**: Quoted field parsing where content is preserved literally except for escape sequences

Key CSV-specific features include:
- **Quoted fields**: Fields enclosed in quote characters (typically double quotes) that can contain delimiters, newlines, and other special characters
- **Escape handling**: Within quoted fields, escape characters can be used to include literal quote or escape characters
- **Flexible delimiters**: Configurable field delimiter, quote character, and escape character
- **Null/default markers**: Support for NULL and DEFAULT value markers (only in unquoted fields)

The parser ensures strict CSV compliance by requiring proper termination of quoted fields and handling edge cases like empty fields and fields containing only whitespace.

## Parameters / Member Variables
- : The COPY operation state containing:
  - : Input line buffer with the raw CSV line to parse
  - : Output buffer for storing parsed field values
  - : Array of pointers to parsed field strings (NULL for null values)
  - : Current capacity of the raw_fields array
  - : The field delimiter character (typically comma)
  - : The field quote character (typically double quote)
  - : The escape character for quoted contexts
  - : String representation of NULL values
  - : String representation of DEFAULT markers
  - : Boolean array indicating which fields should use defaults
  - : Array of default expressions for each column

## Dependencies
- Functions called/Symbols referenced:
  - : Initializes the attribute buffer
  - : Expands buffer capacity as needed
  - : Reallocates the raw_fields array for more columns
  - : Retrieves attribute numbers from the column list
  - : Reports CSV format errors with context
- Called from (representative examples):
  - : Main COPY parsing coordinator function

## Notes and Other Information
- Unlike text format, CSV format does not support backslash escape sequences (\n, \t, etc.) outside of the quote/escape mechanism
- Null and default markers are only recognized in unquoted fields to prevent ambiguity with legitimate quoted content
- The parser uses  statements for efficient state transitions and error handling in the field parsing loop
- Unterminated quoted fields generate specific error messages to help users identify CSV format issues  
- The function maintains the same API as  to allow transparent format switching
- Memory management follows the same optimization strategy as the text parser, pre-allocating buffers to avoid mid-parse reallocations
- The state machine design ensures proper handling of edge cases like adjacent quotes and escape sequences at field boundaries

## Simplified Source

```c
static int
CopyReadAttributesCSV(CopyFromState cstate)
{
    char delimc = cstate->opts.delim[0];
    char quotec = cstate->opts.quote[0];
    char escapec = cstate->opts.escape[0];
    int fieldno;
    char *output_ptr, *cur_ptr, *line_end_ptr;

    // Handle zero-column tables
    if (cstate->max_fields <= 0) {
        if (cstate->line_buf.len != 0)
            ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
                           errmsg("extra data after last expected column")));
        return 0;
    }

    // Setup output buffer and pointers
    resetStringInfo(&cstate->attribute_buf);
    if (cstate->attribute_buf.maxlen <= cstate->line_buf.len)
        enlargeStringInfo(&cstate->attribute_buf, cstate->line_buf.len);

    output_ptr = cstate->attribute_buf.data;
    cur_ptr = cstate->line_buf.data;
    line_end_ptr = cstate->line_buf.data + cstate->line_buf.len;

    // Parse each field
    fieldno = 0;
    for (;;) {
        bool found_delim = false;
        bool saw_quote = false;
        char *start_ptr, *end_ptr;

        // Expand fields array if needed
        if (fieldno >= cstate->max_fields) {
            cstate->max_fields *= 2;
            cstate->raw_fields = repalloc(cstate->raw_fields,
                                        cstate->max_fields * sizeof(char *));
        }

        start_ptr = cur_ptr;
        cstate->raw_fields[fieldno] = output_ptr;

        // State machine: parse field content
        for (;;) {
            // Not in quote mode: look for delimiters and quote starts
            for (;;) {
                end_ptr = cur_ptr;
                if (cur_ptr >= line_end_ptr) goto endfield;

                char c = *cur_ptr++;
                if (c == delimc) {
                    found_delim = true;
                    goto endfield;
                }
                if (c == quotec) {
                    saw_quote = true;
                    break;  // Enter quote mode
                }
                *output_ptr++ = c;
            }

            // In quote mode: handle escape sequences and quote ends
            for (;;) {
                end_ptr = cur_ptr;
                if (cur_ptr >= line_end_ptr)
                    ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
                                   errmsg("unterminated CSV quoted field")));

                char c = *cur_ptr++;

                // Handle escape sequences
                if (c == escapec && cur_ptr < line_end_ptr) {
                    char nextc = *cur_ptr;
                    if (nextc == escapec || nextc == quotec) {
                        *output_ptr++ = nextc;
                        cur_ptr++;
                        continue;
                    }
                }

                // End of quoted field
                if (c == quotec) break;
                *output_ptr++ = c;
            }
        }

endfield:
        *output_ptr++ = '\0';

        // Check for NULL and DEFAULT markers (only in unquoted fields)
        int input_len = end_ptr - start_ptr;
        if (!saw_quote && input_len == cstate->opts.null_print_len &&
            strncmp(start_ptr, cstate->opts.null_print, input_len) == 0) {
            cstate->raw_fields[fieldno] = NULL;
        } else if (fieldno < list_length(cstate->attnumlist) &&
                   cstate->opts.default_print &&
                   input_len == cstate->opts.default_print_len &&
                   strncmp(start_ptr, cstate->opts.default_print, input_len) == 0) {
            // Handle DEFAULT marker
            int m = list_nth_int(cstate->attnumlist, fieldno) - 1;
            if (cstate->defexprs[m] != NULL) {
                cstate->defaults[m] = true;
            } else {
                // Error: column has no default
                TupleDesc tupDesc = RelationGetDescr(cstate->rel);
                Form_pg_attribute att = TupleDescAttr(tupDesc, m);
                ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
                               errmsg("unexpected default marker in COPY data"),
                               errdetail("Column \"%s\" has no default value.",
                                        NameStr(att->attname))));
            }
        }

        fieldno++;
        if (!found_delim) break;  // End of line
    }

    // Finalize output buffer
    output_ptr--;
    cstate->attribute_buf.len = (output_ptr - cstate->attribute_buf.data);

    return fieldno;
}
```