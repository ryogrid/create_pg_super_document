# ColumnsHashData

## Location
src/backend/partitioning/partbounds.c: 4773 - 4783

## Overview
ColumnsHashData is a structure that stores metadata and function information needed for computing hash values of partition key columns in hash partitioning.

## Definition
```c
typedef struct ColumnsHashData
{
    Oid         relid;
    int         nkeys;
    Oid         variadic_type;
    int16       variadic_typlen;
    bool        variadic_typbyval;
    char        variadic_typalign;
    Oid         partcollid[PARTITION_MAX_KEYS];
    FmgrInfo    partsupfunc[FLEXIBLE_ARRAY_MEMBER];
} ColumnsHashData;
```

## Detailed Description
ColumnsHashData is a specialized structure designed to cache and store all the necessary metadata for efficiently computing hash values of partition key columns. This structure is likely used in hash partitioning operations where PostgreSQL needs to repeatedly compute hash values for the same set of columns with the same types and collations. By pre-computing and caching the function manager information and type metadata, PostgreSQL can avoid repeated catalog lookups and type resolution during hash computation.

## Parameters / Member Variables
- `relid`: The OID (object identifier) of the relation this hash data applies to
- `nkeys`: The number of partition key columns
- `variadic_type`: The OID of the variadic type used for hash computation
- `variadic_typlen`: The length of the variadic type (-1 for variable-length types)
- `variadic_typbyval`: Whether the variadic type is passed by value or by reference
- `variadic_typalign`: The alignment requirement for the variadic type
- `partcollid`: Array of collation OIDs for each partition key column (up to PARTITION_MAX_KEYS)
- `partsupfunc`: Array of function manager information for partition support functions (flexible array member)

## Dependencies
- Functions called/Symbols referenced:
  - PARTITION_MAX_KEYS (constant defining maximum partition keys)
  - FLEXIBLE_ARRAY_MEMBER (macro for flexible array members)
  - [FmgrInfo](../F/FmgrInfo.md) (function manager information structure)
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - (No direct references found - likely used internally within partitioning functions)

## Notes and Other Information
This structure appears to be designed for performance optimization in hash partitioning by pre-caching expensive metadata lookups. The flexible array member for partsupfunc allows the structure to accommodate varying numbers of partition support functions. The structure includes detailed type information (length, by-value flag, alignment) which is essential for proper hash computation and memory management.