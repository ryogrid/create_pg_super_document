# requote_identifier

## Location
[src/bin/psql/tab-complete.c:6098-6179](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L6098-L6179)

## Overview
Reconstructs a possibly schema-qualified SQL identifier with proper quoting applied as necessary, serving as the inverse of parse_identifier in psql's tab completion system.

## Definition

```c
static char *
requote_identifier(const char *schemaname, const char *objectname,
				   bool quote_schema, bool quote_object)
```
## Detailed Description
The  function builds a malloc'd string containing a SQL identifier, with quoting applied as necessary. It handles both schema-qualified and simple identifiers, properly escaping double quotes within identifier names by doubling them. The function can handle cases where only a schema name is provided (producing "schema."), or where both schema and object names are provided. Unlike , if an input component was originally quoted, this function will quote the output even when not strictly required, maintaining the original quoting intention.

The function calculates the required buffer size first, accounting for:
- The length of schema and object names
- Additional characters for dots, quote marks, and null terminator
- Extra characters needed for escaping internal double quotes

## Parameters / Member Variables
- : The schema name component of the identifier (can be NULL)
- : The object name component of the identifier (can be NULL)
- : Boolean flag indicating whether to force quoting of the schema name
- : Boolean flag indicating whether to force quoting of the object name

## Dependencies
- Functions called/Symbols referenced:
  - [identifier_needs_quotes](../i/identifier_needs_quotes.md) (called twice to determine if quoting is needed)
  - [pg_malloc](../p/pg_malloc.md) (for memory allocation)
- Called from (representative examples):
  - THING_NO_SHOW (completion handling)
  - [_complete_from_query](../c/_complete_from_query.md) (query-based completion)

## Notes and Other Information
- Returns a malloc'd string that must be freed by the caller
- Properly handles SQL identifier quoting rules by doubling internal double quotes
- Part of psql's tab completion system in PostgreSQL
- Located in src/bin/psql/tab-complete.c at lines 6098-6179
- The function is static, meaning it's only accessible within the tab-complete.c file
- Can produce schema-only output ("schema.") when objectname is NULL but schemaname is provided