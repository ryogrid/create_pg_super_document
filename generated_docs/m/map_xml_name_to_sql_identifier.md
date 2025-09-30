# map_xml_name_to_sql_identifier

## Location
[src/backend/utils/adt/xml.c:2436-2477](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L2436-L2477)

## Overview
Maps XML names back to SQL identifiers by reversing the character escaping performed by , implementing SQL/XML:2008 section 9.3.

## Definition

```c
struct_array(array, elmtype,
						  elmlen, elmbyval, elmalign,
						  &elem_values, &elem_nulls,
						  &num_elems);
```
## Detailed Description
This function converts XML names back to their original SQL identifier form by decoding escape sequences that were applied during the XML name mapping process. It specifically looks for the Unicode escape pattern  where NNNN represents a 4-digit hexadecimal Unicode code point, and converts these back to their original characters.

The function processes the input character by character, detecting escape sequences and converting them back to their original Unicode characters using the server's character encoding. Characters that were not escaped are copied directly to the output.

This function serves as the inverse operation to , allowing for round-trip conversion between SQL identifiers and XML names.

## Parameters
- : The XML name string to be converted back to a SQL identifier

## Dependencies
- Functions called/Symbols referenced:
  - : Initialize string buffer for building result
  - : Get multibyte character length for proper iteration
  - : Check if character is a hexadecimal digit (from standard C library)
  - : Parse hexadecimal Unicode value from escape sequence
  - : Convert Unicode code point to server encoding
  - : Append decoded character to result buffer
  - : Append original character bytes when no decoding needed
  - : Maximum length constant for Unicode character conversion

- Called from (representative examples):
  - : Rule expression formatting in ruleutils
  - Functions that need to reverse XML name mapping back to SQL identifiers

## Notes and Other Information
- Returns a newly allocated string that must be freed by the caller
- Only processes escape sequences in the exact format  where N is a hexadecimal digit
- Supports multibyte character encodings through proper use of 
- The function assumes the input was previously processed by  or follows the same escape sequence format
- Characters that don't match the escape pattern are copied unchanged, making the function safe to use on strings that may not contain escape sequences

## Simplified Source

```c
char *map_xml_name_to_sql_identifier(const char *name)
{
    StringInfoData buf;
    const char *p;

    initStringInfo(&buf);

    // Process each character/multibyte sequence
    for (p = name; *p; p += pg_mblen(p)) {
        // Check for Unicode escape sequence: _xNNNN_
        if (*p == '_' && *(p + 1) == 'x' &&
            isxdigit((unsigned char) *(p + 2)) &&
            isxdigit((unsigned char) *(p + 3)) &&
            isxdigit((unsigned char) *(p + 4)) &&
            isxdigit((unsigned char) *(p + 5)) &&
            *(p + 6) == '_') {

            // Decode the Unicode escape sequence
            char cbuf[MAX_UNICODE_EQUIVALENT_STRING + 1];
            unsigned int u;

            // Parse the 4-digit hex value
            sscanf(p + 2, "%X", &u);

            // Convert Unicode to server encoding
            pg_unicode_to_server(u, (unsigned char *) cbuf);
            appendStringInfoString(&buf, cbuf);

            // Skip past the entire escape sequence
            p += 6;
        } else {
            // Copy character as-is (no escape sequence)
            appendBinaryStringInfo(&buf, p, pg_mblen(p));
        }
    }

    return buf.data;
}
```