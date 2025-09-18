# pg_ts_parser_is_visible

## Location
[src/backend/catalog/namespace.c:5020-5033](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L5020-L5033)

## Overview
Determines whether a given text search parser is visible in the current search path, returning NULL if the parser does not exist.

## Definition
```c
Datum pg_ts_parser_is_visible(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL SQL-callable function that checks the visibility of a text search parser identified by its OID in the current search path. It serves as a wrapper around the internal TSParserIsVisibleExt function, providing a SQL interface for text search parser visibility checks. The function returns a boolean value indicating whether the parser is accessible from the current namespace context, or NULL if the parser doesn't exist in the system catalogs.

The visibility check considers the current search path and ensures that the parser would be found by name resolution. A text search parser is considered visible if it exists in a namespace that's in the current search path and wouldn't be shadowed by another parser with the same name in an earlier namespace. Text search parsers are components of PostgreSQL's full-text search system that break documents into tokens.

## Parameters / Member Variables
- First argument (OID): The object identifier of the text search parser to check for visibility

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID: Extracts OID argument from function call
  - [TSParserIsVisibleExt](../T/TSParserIsVisibleExt.md): Internal function that performs the actual visibility check
  - PG_RETURN_NULL: Returns NULL value to SQL caller
  - PG_RETURN_BOOL: Returns boolean value to SQL caller
- Called from (representative examples):
  - Available as SQL function pg_ts_parser_is_visible()

## Notes and Other Information
- This is a system information function available in SQL as pg_ts_parser_is_visible(oid)
- Returns NULL rather than FALSE when the parser doesn't exist, following PostgreSQL's convention for visibility functions
- The function uses the is_missing parameter of TSParserIsVisibleExt to distinguish between "not visible" and "doesn't exist"
- Part of PostgreSQL's namespace and visibility system for schema-qualified object resolution
- Text search parsers are part of PostgreSQL's full-text search infrastructure and are used to tokenize documents
- The underlying visibility check explicitly excludes temporary namespaces from the search
- Located in src/backend/catalog/namespace.c:5020-5033