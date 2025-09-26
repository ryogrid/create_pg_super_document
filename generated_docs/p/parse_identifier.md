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