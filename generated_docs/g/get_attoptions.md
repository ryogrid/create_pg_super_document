# get_attoptions

## Location
src/backend/utils/cache/lsyscache.c: 970 - 1006

## Overview
Retrieves the attribute options text[] datum for a specific column in a PostgreSQL relation, providing access to column-level storage and behavioral options.

## Definition


## Detailed Description
The `get_attoptions` function looks up and returns the attribute options (attoptions) for a specific column within a PostgreSQL relation. These options are stored as a text[] array in the pg_attribute system catalog and contain column-specific storage parameters and behavioral settings. The function performs a system cache lookup to efficiently retrieve this information, returning a copied datum to ensure safe memory management.

The function uses the PostgreSQL system cache mechanism (ATTNUM cache) to perform an efficient lookup based on the relation OID and attribute number. If the attribute is found but has no options set, the function returns a null datum (0). If options exist, it creates a copy of the datum to ensure the caller owns the memory.

## Parameters / Member Variables
- `relid`: The OID of the relation containing the target attribute
- `attnum`: The attribute number (column number) within the relation, starting from 1

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache2 (performs system cache lookup)
  - ObjectIdGetDatum (converts OID to Datum)
  - Int16GetDatum (converts int16 to Datum)
  - SysCacheGetAttr (extracts attribute from cached tuple)
  - datumCopy (creates a copy of the datum)
  - ReleaseSysCache (releases cache reference)
- Called from (representative examples):
  - index_concurrently_create_copy
  - CheckIndexCompatible
  - generateClonedIndexStmt
  - transformIndexConstraint
  - pg_get_indexdef_worker
  - RelationGetIndexAttOptions

## Notes and Other Information
- The function will throw an ERROR if the specified attribute does not exist in the system cache
- Returns 0 (null datum) when the attribute exists but has no options configured
- The returned datum is a copy, so the caller is responsible for memory management
- Attribute options are stored as text[] arrays and typically contain storage parameters like fillfactor, compression settings, etc.
- This function is commonly used during index operations and DDL command processing where column-specific options need to be preserved or validated