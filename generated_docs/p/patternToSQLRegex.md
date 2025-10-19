# patternToSQLRegex

## Location
[src/fe_utils/string_utils.c:1225-1393](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/string_utils.c#L1225-L1393)

## Overview
Transforms a shell-style object name pattern into SQL-style regular expressions, handling qualified names, case conversion, and wildcard character mapping.

## Definition

```c
void
patternToSQLRegex(int encoding, PQExpBuffer dbnamebuf, PQExpBuffer schemabuf,
				  PQExpBuffer namebuf, const char *pattern, bool force_escape,
				  bool want_literal_dbname, int *dotcnt)
```
## Detailed Description
This complex function converts shell-style patterns (with wildcards like * and ?) into PostgreSQL-compatible regular expressions. It can parse qualified object names with up to three components (database.schema.name) and handles various formatting requirements:

Key transformations:
- Converts shell wildcards: '*' → '.*', '?' → '.'  
- Lower-cases unquoted uppercase letters
- Escapes regex special characters when inside quotes or when force_escape is true
- Handles quoted identifiers with PostgreSQL-style double-quote escaping
- Splits qualified names at dots into separate components
- Wraps results in '^(...)$' for whole-string matching

The function intelligently distributes pattern components across the provided buffers based on the number of separators found and available output buffers.

## Parameters / Member Variables
- `encoding`: Character encoding for the input pattern
- `dbnamebuf`: Output buffer for database name pattern (can be NULL)
- `schemabuf`: Output buffer for schema name pattern (can be NULL)
- `namebuf`: Output buffer for object name pattern (can be NULL)
- `*pattern`: Input shell-style pattern string
- `force_escape`: If true, always escape regex special characters
- `want_literal_dbname`: If true, don't convert dbname to regex format
- `*dotcnt`: Output parameter receiving count of separators found
## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [pg_tolower](pg_tolower.md)
  - [PQmblenBounded](../P/PQmblenBounded.md)
  - strchr
  - isupper

- Called from (representative examples):
  - [append_database_pattern](../a/append_database_pattern.md) (src/bin/pg_amcheck/pg_amcheck.c:1363)
  - [append_schema_pattern](../a/append_schema_pattern.md) (src/bin/pg_amcheck/pg_amcheck.c:1396)
  - [append_relation_pattern_helper](../a/append_relation_pattern_helper.md) (src/bin/pg_amcheck/pg_amcheck.c:1441)
  - WHEREAND (src/fe_utils/string_utils.c:1093)

## Notes and Other Information
- Located in src/fe_utils/string_utils.c:1225-1393
- Handles multi-byte character encodings through PQmblenBounded
- Always quotes '$' characters as they're valid in SQL identifiers
- Special handling for '[]' patterns to avoid regex bracket expressions
- Callers should check dotcnt return value for error handling when too many dots are present
- Used primarily by PostgreSQL utilities for pattern matching in database object queries
- [Complex](../C/Complex.md) state machine handles quote processing and component separation
- Part of frontend utilities library for advanced pattern processing

## Simplified Source

```c
void patternToSQLRegex(int encoding, PQExpBuffer dbnamebuf, PQExpBuffer schemabuf,
                      PQExpBuffer namebuf, const char *pattern, bool force_escape,
                      bool want_literal_dbname, int *dotcnt) {
    PQExpBufferData buf[3];
    PQExpBuffer curbuf = &buf[0];
    PQExpBuffer maxbuf;
    bool inquotes = false;
    const char *cp = pattern;

    *dotcnt = 0;

    // Determine maximum buffer based on available output buffers
    if (dbnamebuf != NULL) maxbuf = &buf[2];
    else if (schemabuf != NULL) maxbuf = &buf[1];
    else maxbuf = &buf[0];

    // Initialize current buffer with regex anchor
    initPQExpBuffer(curbuf);
    appendPQExpBufferStr(curbuf, "^(");

    // Process each character in the pattern
    while (*cp) {
        char ch = *cp;

        if (ch == '"') {
            // Handle quoted identifier escaping
            if (inquotes && cp[1] == '"') {
                appendPQExpBufferChar(curbuf, '"');  // Escaped quote
                cp++;
            } else {
                inquotes = !inquotes;  // Toggle quote state
            }
        }
        else if (!inquotes && isupper((unsigned char) ch)) {
            // Convert unquoted uppercase to lowercase
            appendPQExpBufferChar(curbuf, pg_tolower((unsigned char) ch));
        }
        else if (!inquotes && ch == '*') {
            // Shell wildcard: * -> .*
            appendPQExpBufferStr(curbuf, ".*");
        }
        else if (!inquotes && ch == '?') {
            // Shell wildcard: ? -> .
            appendPQExpBufferChar(curbuf, '.');
        }
        else if (!inquotes && ch == '.') {
            // Component separator - switch to next buffer if available
            (*dotcnt)++;
            if (curbuf < maxbuf) {
                appendPQExpBufferStr(curbuf, ")$");
                curbuf++;
                initPQExpBuffer(curbuf);
                appendPQExpBufferStr(curbuf, "^(");
            } else {
                appendPQExpBufferChar(curbuf, '.');  // Literal dot
            }
        }
        else if (ch == '$') {
            // Always escape $ (valid in SQL identifiers)
            appendPQExpBufferStr(curbuf, "\\$");
        }
        else {
            // Regular character - escape regex specials if needed
            if ((inquotes || force_escape) && strchr("|*+?()[]{}.^$\\", ch)) {
                appendPQExpBufferChar(curbuf, '\\');
            }
            appendPQExpBufferChar(curbuf, ch);
        }
        cp++;
    }

    // Close final regex anchor
    appendPQExpBufferStr(curbuf, ")$");

    // Distribute results to output buffers (rightmost first)
    if (namebuf && curbuf >= buf) {
        appendPQExpBufferStr(namebuf, curbuf->data);
        curbuf--;
    }
    if (schemabuf && curbuf >= buf) {
        appendPQExpBufferStr(schemabuf, curbuf->data);
        curbuf--;
    }
    if (dbnamebuf && curbuf >= buf) {
        appendPQExpBufferStr(dbnamebuf, curbuf->data);
    }
}
```