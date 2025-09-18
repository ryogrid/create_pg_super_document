# gtsquery_compress

## Location
src/backend/utils/adt/tsquery_gist.c: 27 - 52

## Overview
gtsquery_compress is a GiST (Generalized Search Tree) compression function that compresses TSQuery (text search query) values for efficient storage and searching in GiST indexes.

## Definition


## Detailed Description
This function is part of PostgreSQL's GiST index operator class for TSQuery data types. It performs compression by converting leaf-level TSQuery values into compact TSQuerySign signatures. For non-leaf entries, it returns the entry unchanged. The compression creates a lossy but space-efficient representation that maintains the essential search characteristics needed for index operations.

When processing leaf keys, the function:
1. Extracts the TSQuery from the GISTENTRY
2. Generates a TSQuerySign using makeTSQuerySign()
3. Creates a new GISTENTRY containing the compressed signature
4. Returns the compressed entry for storage in the index

## Parameters / Member Variables
- : Pointer to GISTENTRY containing the TSQuery value to be compressed
- : Pointer to the resulting GISTENTRY (either original or newly created compressed version)

## Dependencies
- Functions called/Symbols referenced:
  - [GISTENTRY](../G/GISTENTRY.md) (struct type)
  - TSQuerySign (type)
  - [makeTSQuerySign](../m/makeTSQuerySign.md) (creates signature from TSQuery)
  - [DatumGetTSQuery](../D/DatumGetTSQuery.md) (extracts TSQuery from Datum)
  - gistentryinit (initializes GISTENTRY structure)
  - [TSQuerySignGetDatum](../T/TSQuerySignGetDatum.md) (converts signature to Datum)
  - [palloc](../p/palloc.md) (memory allocation)
- Called from (representative examples):
  - GiST index operations (no direct references found in codebase)

## Notes and Other Information
- This is a PostgreSQL extension function that follows the PG_FUNCTION_ARGS convention
- Only compresses leaf keys; internal nodes are passed through unchanged
- The compression is lossy but preserves search semantics for GiST index operations
- Memory for the new GISTENTRY is allocated using palloc() when compression occurs
- Part of the TSQuery GiST operator class implementation in src/backend/utils/adt/tsquery_gist.c