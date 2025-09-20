# splitTableColumnsSpec

## Location
[src/bin/scripts/common.c:34-68](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/scripts/common.c#L34-L68)

## Overview
Parses a table specification string in the format TABLE[(COLUMNS)] and splits it into separate table name and column specification components.

## Definition

```c
void
splitTableColumnsSpec(const char *spec, int encoding,
					  char **table, const char **columns)
```
## Detailed Description
This function takes a string specification that may contain a table name followed by an optional column list in parentheses, and splits it into two parts: the table name portion and the columns portion. The function handles identifier quoting properly by tracking double-quoted sections and ignores parentheses that appear within quoted identifiers.

The function scans through the input string character by character, maintaining a quote state to handle PostgreSQL identifier quoting rules. It stops at the first unquoted opening parenthesis '(' or at the end of the string if no parentheses are found. The table name is extracted as everything up to this point, while the columns portion points to the remaining part of the original string (including the parentheses if present).

## Parameters / Member Variables
- : Input specification string in format TABLE[(COLUMNS)]
- : Character encoding to use for multi-byte character handling
- : Output parameter that receives a newly allocated copy of the table name portion (must be freed with pg_free)
- : Output parameter that receives a pointer into the original spec string pointing to the column specification (or to the NUL terminator if no columns)

## Dependencies
- Functions called/Symbols referenced:
  - [PQmblenBounded](../P/PQmblenBounded.md)
  - [pnstrdup](../p/pnstrdup.md)
- Called from (representative examples):
  - [appendQualifiedRelation](../a/appendQualifiedRelation.md)
  - [vacuum_one_database](../v/vacuum_one_database.md)

## Notes and Other Information
- The caller is responsible for freeing the memory allocated for the table name using pg_free()
- The columns pointer references the original spec string and should not be freed separately
- Handles PostgreSQL identifier quoting rules where double quotes can be escaped by doubling them
- Uses PQmblenBounded for proper multi-byte character support based on the provided encoding
- If no column specification is present, the columns pointer will point to the NUL terminator of the spec string