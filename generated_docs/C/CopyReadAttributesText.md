# CopyReadAttributesText

## Location
[src/backend/commands/copyfromparse.c:1537-1790](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfromparse.c#L1537-L1790)

## Overview
Parses a single line of text-format COPY data into separate attribute fields, performing character de-escaping and handling null/default markers according to PostgreSQL's text format specifications.

## Definition

```c
static int
CopyReadAttributesText(CopyFromState cstate)
```
## Detailed Description
This function is the core parser for text-format COPY operations in PostgreSQL. It processes the current input line stored in , separating it into individual field values based on the configured delimiter character. The function performs sophisticated escape sequence processing, converting backslash-escaped characters (like , , octal sequences , and hexadecimal sequences ) back to their literal values.

The function handles three types of field values:
1. **Regular data**: Standard field content with escape processing
2. **NULL markers**: Fields matching the configured null representation string
3. **DEFAULT markers**: Fields matching the configured default value marker (when defaults are enabled)

Key features include:
- Efficient single-pass parsing with speculative de-escaping
- Dynamic memory management for variable field counts
- Character encoding validation for non-ASCII characters
- Support for zero-column tables
- Robust error handling with meaningful error messages

The parsed field values are stored as null-terminated strings in , with NULL pointers indicating null values.

## Parameters / Member Variables
- : The COPY operation state containing:
  - : Input line buffer containing the raw text to parse
  - : Output buffer for storing de-escaped field values
  - : Array of pointers to parsed field strings (NULL for null values)
  - : Current size of the raw_fields array (dynamically expanded)
  - : The field delimiter character
  - : String representation of NULL values
  - : String representation of DEFAULT markers
  - : Boolean array tracking which fields use default values
  - : Array of default value expressions for each column

## Dependencies
- Functions called/Symbols referenced:
  - : Clears the attribute buffer
  - : Expands buffer capacity
  - : Reallocates the raw_fields array
  - : Macros for octal digit processing
  - : Converts hex digits to decimal
  - : Checks for non-ASCII characters
  - : Retrieves attribute numbers from column list
  - : Validates multi-byte character encoding
- Called from (representative examples):
  - : Main COPY parsing coordinator function

## Notes and Other Information
- The function uses speculative de-escaping, performing escape processing while scanning but only committing the results after null/default marker checks
- Memory management is optimized to avoid frequent reallocations by pre-sizing buffers based on input length
- The parser supports PostgreSQL's full escape sequence syntax including octal (\001-\377) and hexadecimal (\x00-\xFF) representations
- Non-ASCII characters generated through escape sequences trigger encoding validation to ensure database compatibility
- Zero-column table handling is a special case that simply validates empty input lines
- Error reporting includes detailed context about column names and expected formats for better user experience

## Simplified Source

```c
static int
CopyReadAttributesText(CopyFromState cstate)
{
    char delimc = cstate->opts.delim[0];
    int fieldno;
    char *output_ptr, *cur_ptr, *line_end_ptr;

    // Handle zero-column tables
    if (cstate->max_fields <= 0) {
        if (cstate->line_buf.len != 0)
            ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
                           errmsg("extra data after last expected column")));
        return 0;
    }

    // Setup output buffer
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
        char *start_ptr, *end_ptr;
        bool saw_non_ascii = false;

        // Expand fields array if needed
        if (fieldno >= cstate->max_fields) {
            cstate->max_fields *= 2;
            cstate->raw_fields = repalloc(cstate->raw_fields,
                                        cstate->max_fields * sizeof(char *));
        }

        start_ptr = cur_ptr;
        cstate->raw_fields[fieldno] = output_ptr;

        // Parse field with escape sequence handling
        for (;;) {
            char c;
            end_ptr = cur_ptr;
            if (cur_ptr >= line_end_ptr) break;

            c = *cur_ptr++;
            if (c == delimc) {
                found_delim = true;
                break;
            }

            // Handle escape sequences
            if (c == '\\') {
                if (cur_ptr >= line_end_ptr) break;
                c = *cur_ptr++;

                switch (c) {
                    case '0': case '1': case '2': case '3':
                    case '4': case '5': case '6': case '7':
                        // Octal sequences (\013)
                        {
                            int val = OCTVALUE(c);
                            if (cur_ptr < line_end_ptr && ISOCTAL(*cur_ptr)) {
                                val = (val << 3) + OCTVALUE(*cur_ptr++);
                                if (cur_ptr < line_end_ptr && ISOCTAL(*cur_ptr))
                                    val = (val << 3) + OCTVALUE(*cur_ptr++);
                            }
                            c = val & 0377;
                            if (c == '\0' || IS_HIGHBIT_SET(c))
                                saw_non_ascii = true;
                        }
                        break;
                    case 'x':
                        // Hexadecimal sequences (\x3F)
                        if (cur_ptr < line_end_ptr && isxdigit((unsigned char) *cur_ptr)) {
                            int val = GetDecimalFromHex(*cur_ptr++);
                            if (cur_ptr < line_end_ptr && isxdigit((unsigned char) *cur_ptr))
                                val = (val << 4) + GetDecimalFromHex(*cur_ptr++);
                            c = val & 0xff;
                            if (c == '\0' || IS_HIGHBIT_SET(c))
                                saw_non_ascii = true;
                        }
                        break;
                    case 'b': c = '\b'; break;
                    case 'f': c = '\f'; break;
                    case 'n': c = '\n'; break;
                    case 'r': c = '\r'; break;
                    case 't': c = '\t'; break;
                    case 'v': c = '\v'; break;
                    // All other cases: take character literally
                }
            }

            *output_ptr++ = c;
        }

        // Check for NULL and DEFAULT markers
        int input_len = end_ptr - start_ptr;
        if (input_len == cstate->opts.null_print_len &&
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
                TupleDesc tupDesc = RelationGetDescr(cstate->rel);
                Form_pg_attribute att = TupleDescAttr(tupDesc, m);
                ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
                               errmsg("unexpected default marker in COPY data"),
                               errdetail("Column \"%s\" has no default value.",
                                        NameStr(att->attname))));
            }
        } else {
            // Validate encoding for non-ASCII characters
            if (saw_non_ascii) {
                char *fld = cstate->raw_fields[fieldno];
                pg_verifymbstr(fld, output_ptr - fld, false);
            }
        }

        *output_ptr++ = '\0';
        fieldno++;
        if (!found_delim) break;
    }

    // Finalize output buffer
    output_ptr--;
    cstate->attribute_buf.len = (output_ptr - cstate->attribute_buf.data);

    return fieldno;
}
```