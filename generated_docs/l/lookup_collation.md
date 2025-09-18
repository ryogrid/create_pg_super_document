# lookup_collation

## Location
src/backend/catalog/namespace.c: 2322 - 2372

## Overview
A static utility function that searches for a collation by name within a specific namespace, ensuring the collation is compatible with the given character encoding.

## Definition
```c
static Oid lookup_collation(const char *collname, Oid collnamespace, int32 encoding)
```

## Detailed Description
This function performs a two-stage lookup for collations in PostgreSQL's system catalog. It first searches for an exact match with the specific encoding, then falls back to searching for collations that support any encoding (-1). For ICU collations found in the second stage, it validates that the requested encoding is supported by ICU before returning the collation OID.

The function is critical for PostgreSQL's collation resolution mechanism, ensuring that only compatible collation-encoding combinations are used in database operations.

## Parameters / Member Variables
- `collname`: The name of the collation to look up
- `collnamespace`: The OID of the namespace (schema) where the collation should be found
- `encoding`: The character encoding ID that the collation must support

## Dependencies
- Functions called/Symbols referenced:
  - GetSysCacheOid3 (for exact encoding match lookup)
  - [SearchSysCache3](../S/SearchSysCache3.md) (for any-encoding collation lookup)
  - [is_encoding_supported_by_icu](../i/is_encoding_supported_by_icu.md) (to validate ICU encoding compatibility)
  - Form_pg_collation (system catalog structure)
  - COLLPROVIDER_ICU (collation provider constant)
- Called from (representative examples):
  - [CollationGetCollid](../C/CollationGetCollid.md)
  - [get_collation_oid](../g/get_collation_oid.md)

## Notes and Other Information
- This is a static function, only visible within namespace.c
- Implements a fallback strategy: specific encoding first, then any-encoding with validation
- ICU collations require special encoding compatibility checking, while libc collations with encoding -1 work with all encodings
- Returns InvalidOid when no compatible collation is found
- Uses PostgreSQL's system cache for efficient catalog lookups