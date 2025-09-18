# gettype

## Location
src/backend/bootstrap/bootstrap.c: 735 - 805

## Overview
A static function that looks up type information by name, returning either an index into the TypInfo array or a real OID depending on the current state of the type cache.

## Definition


## Detailed Description
This function implements a two-phase type lookup mechanism during PostgreSQL bootstrap. Initially, when the Typ list is empty (NIL), it searches the statically defined TypInfo array and returns an array index. Once a type not found in TypInfo is encountered, it populates the Typ list by reading pg_type catalog and switches to returning actual OIDs.

The function has complex behavior that depends on the global state:
1. If Typ is NIL: searches TypInfo array, returns index, and if not found, populates Typ list and recurses
2. If Typ is populated: searches the cached type list, returns OID, and if not found, refreshes the cache and searches again

This design allows bootstrap to work with a minimal set of built-in types initially, while gracefully handling the transition to full catalog-based type lookup when needed.

## Parameters / Member Variables
- : The name of the type to look up (null-terminated string)

## Dependencies
- Functions called/Symbols referenced:
  - populate_typ_list (to load type cache from pg_type)
  - list_free_deep (to free existing type list)
  - gettype (recursive call after populating Typ list)
  - strncmp (for string comparison)
  - elog (for error and debug logging)
  - NameStr (macro to extract name from Name type)
  - NAMEDATALEN (constant for maximum name length)
  - DEBUG4 (debug log level)

- Called from:
  - DefineAttr (when defining attributes during bootstrap)
  - gettype (recursive self-call)

## Notes and Other Information
- Sets global variable Ap to point to the found typmap entry when successful
- The function's return value semantics change based on whether Typ is populated
- Callers must check if Typ is NIL to interpret the return value correctly
- Handles composite types by refreshing the type cache when a type is not found
- Uses NAMEDATALEN for safe string comparison to avoid buffer overflows
- The two-phase approach optimizes bootstrap performance by avoiding catalog access for common types
- Recursion is used carefully to avoid infinite loops when types are missing from pg_type