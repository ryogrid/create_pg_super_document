# TSParserIsVisibleExt

## Location
src/backend/catalog/namespace.c: 2786 - 2860

## Overview
Extended version of TSParserIsVisible that determines parser visibility in the search path with optional missing object handling.

## Definition
```c
static bool TSParserIsVisibleExt(Oid prsId, bool *is_missing)
```

## Detailed Description
This function performs the core logic for determining whether a text search parser is visible in the current search path. It implements a two-phase visibility check:

1. **Path membership check**: Quickly determines if the parser's namespace is in the active search path at all. System catalog objects (PG_CATALOG_NAMESPACE) are always considered to be in the path.

2. **Name conflict resolution**: If the namespace is in the path, performs a detailed check to ensure no other parser with the same name appears earlier in the search path, which would shadow this parser.

The function handles missing parsers gracefully when the is_missing parameter is provided, setting it to true and returning false instead of throwing an error.

## Parameters
- `prsId`: The OID of the text search parser to check for visibility
- `is_missing`: Optional pointer to bool; if provided and parser doesn't exist, sets *is_missing = true instead of throwing error (caller must initialize to false)

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_ts_parser (struct type)
  - recomputeNamespacePath
  - list_member_oid
  - SearchSysCacheExists2
- Called from (representative examples):
  - TSParserIsVisible
  - pg_ts_parser_is_visible

## Notes and Other Information
- Static function, only accessible within namespace.c
- Uses TSPARSEROID system cache to look up parser details
- Skips temporary namespaces during search path traversal
- System catalog parsers (PG_CATALOG_NAMESPACE) are always considered visible if they exist
- Implements proper namespace shadowing semantics - earlier entries in search path hide later ones with same name
- Part of PostgreSQL's visibility infrastructure for text search objects
- Located in src/backend/catalog/namespace.c at lines 2786-2860