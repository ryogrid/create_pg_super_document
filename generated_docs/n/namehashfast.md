# namehashfast

## Location
[src/backend/utils/cache/catcache.c:212-219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L212-L219)

## Overview
The `namehashfast` function provides a fast hash function for PostgreSQL's `name` data type, used internally by the catalog cache system for efficient hash table operations on name-based keys.

## Definition
```c
static uint32 namehashfast(Datum datum)
```

## Detailed Description
This function generates hash values for PostgreSQL `name` data types within the catalog cache system. It extracts a C-style string from the input Datum using the `NameStr` macro and `DatumGetName`, then applies the `hash_any` function to compute a 32-bit hash value based on the actual string length. This approach ensures consistent hashing for variable-length name strings while maintaining optimal performance for catalog cache hash table operations.

## Parameters / Member Variables
- `datum`: A Datum containing a name value to be hashed

## Dependencies
- Functions called/Symbols referenced:
  - `[DatumGetName](../D/DatumGetName.md)`: Extracts Name pointer from Datum
  - `[hash_any](../h/hash_any.md)`: Standard PostgreSQL hash function for arbitrary byte sequences
  - `strlen`: Standard C library function to determine string length
- Called from (representative examples):
  - `[GetCCHashEqFuncs](../G/GetCCHashEqFuncs.md)`: Function that retrieves hash and equality functions for catalog cache

## Notes and Other Information
- Uses `hash_any` with dynamic string length rather than fixed `NAMEDATALEN`
- The `NameStr` macro converts Name pointer to null-terminated C-string
- Optimized for catalog cache performance where name-based hashing is critical
- Part of PostgreSQL's internal catalog cache hash table infrastructure
- Static function scope limits usage to catcache.c compilation unit
- Complements `nameeqfast` for complete name-based hash table operations

## Simplified Source

```c
static uint32 namehashfast(Datum datum) {
    // Extract C-string from Name Datum
    char *key = NameStr(*DatumGetName(datum));

    // Compute hash using actual string length
    return hash_any((unsigned char *) key, strlen(key));
}
```