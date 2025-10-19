# parse_identifier

## Location
[src/bin/psql/tab-complete.c:5999-6097](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L5999-L6097)

## Overview
Parses a potentially schema-qualified SQL identifier, handling quoting, downcasing, and schema separation according to PostgreSQL identifier rules.

## Definition
static void parse_identifier(const char *ident, char **schemaname, char **objectname, bool *schemaquoted, bool *objectquoted)

## Detailed Description
This function decomposes a SQL identifier that may include schema qualification (schema.object) into separate components. It handles PostgreSQL's identifier quoting rules, including double-quote processing for case-sensitive identifiers and automatic downcasing of unquoted portions. The function is more permissive than the backend parser, allowing partial quoting within identifiers to accommodate psql metacommand traditions.

The parser correctly handles escape sequences within quoted identifiers (double quotes represented as "") and multibyte character sequences in client encodings. It performs downcasing transformations that approximate the backend's downcase_identifier() function, though locale differences between client and server may cause minor variations.

## Parameters / Member Variables
- ident: Input identifier string to parse (potentially schema-qualified)
- schemaname: Output pointer for malloc'd schema name (NULL if no schema)  
- objectname: Output pointer for malloc'd object name
- schemaquoted: Output boolean indicating if schema part was quoted
- objectquoted: Output boolean indicating if object part was quoted

## Dependencies
- Functions called/Symbols referenced:
  - strlen
  - [pg_encoding_max_length](pg_encoding_max_length.md)
  - [pg_malloc](pg_malloc.md)
  - IS_HIGHBIT_SET
  - [PQmblenBounded](../P/PQmblenBounded.md)
  - free
  - tolower
  - isupper
- Called from (representative examples):
  - [_complete_from_query](../c/_complete_from_query.md)
  - [set_completion_reference](../s/set_completion_reference.md)
  - THING_NO_SHOW completion system

## Notes and Other Information
The function allocates memory for output strings that must be freed by the caller. It handles catalog.schema.object patterns by dropping catalog names and keeping only schema.object. Multibyte character processing ensures safe operation across different client encodings. The downcasing behavior attempts to match PostgreSQL's backend identifier processing but may differ due to locale variations.

## Simplified Source

```c
static void
parse_identifier(const char *ident,
                char **schemaname, char **objectname,
                bool *schemaquoted, bool *objectquoted)
{
    size_t buflen = strlen(ident) + 1;
    bool enc_is_single_byte = (pg_encoding_max_length(pset.encoding) == 1);
    char *sname = NULL;
    char *oname = pg_malloc(buflen);
    char *optr = oname;
    bool inquotes = false;

    // Initialize output flags
    *schemaquoted = *objectquoted = false;

    // Parse the identifier character by character
    while (*ident) {
        unsigned char ch = (unsigned char) *ident++;

        if (ch == '"') {
            if (inquotes && *ident == '"') {
                // Double quote within quoted identifier = literal quote
                *optr++ = '"';
                ident++;
            } else {
                // Toggle quote state
                inquotes = !inquotes;
                *objectquoted = true;
            }
        } else if (ch == '.' && !inquotes) {
            // Found schema separator - move current name to schema
            *optr = '\0';
            free(sname);  // Drop any catalog name
            sname = oname;
            oname = pg_malloc(buflen);
            optr = oname;
            *schemaquoted = *objectquoted;
            *objectquoted = false;
        } else if (!enc_is_single_byte && IS_HIGHBIT_SET(ch)) {
            // Handle multibyte characters safely
            int chlen = PQmblenBounded(ident - 1, pset.encoding);
            *optr++ = (char) ch;
            while (--chlen > 0)
                *optr++ = *ident++;
        } else {
            // Regular character - downcase if not quoted
            if (!inquotes) {
                if (ch >= 'A' && ch <= 'Z')
                    ch += 'a' - 'A';
                else if (enc_is_single_byte && IS_HIGHBIT_SET(ch) && isupper(ch))
                    ch = tolower(ch);
            }
            *optr++ = (char) ch;
        }
    }

    // Finalize outputs
    *optr = '\0';
    *schemaname = sname;
    *objectname = oname;
}
```