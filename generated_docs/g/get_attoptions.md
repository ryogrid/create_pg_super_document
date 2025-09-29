# get_attoptions

## Location
[src/backend/utils/cache/lsyscache.c:970-1006](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L970-L1006)

## Overview
Retrieves the attribute options text[] datum for a specific column in a PostgreSQL relation, providing access to column-level storage and behavioral options.

## Definition

```c
Datum
get_attoptions(Oid relid, int16 attnum)
```
## Detailed Description
The `get_attoptions` function looks up and returns the attribute options (attoptions) for a specific column within a PostgreSQL relation. These options are stored as a text[] array in the pg_attribute system catalog and contain column-specific storage parameters and behavioral settings. The function performs a system cache lookup to efficiently retrieve this information, returning a copied datum to ensure safe memory management.

The function uses the PostgreSQL system cache mechanism (ATTNUM cache) to perform an efficient lookup based on the relation OID and attribute number. If the attribute is found but has no options set, the function returns a null datum (0). If options exist, it creates a copy of the datum to ensure the caller owns the memory.

## Parameters / Member Variables
- `relid`: The OID of the relation containing the target attribute
- `attnum`: The attribute number (column number) within the relation, starting from 1

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache2](../S/SearchSysCache2.md) (performs system cache lookup)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (converts OID to Datum)
  - [Int16GetDatum](../I/Int16GetDatum.md) (converts int16 to Datum)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md) (extracts attribute from cached tuple)
  - [datumCopy](../d/datumCopy.md) (creates a copy of the datum)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (releases cache reference)
- Called from (representative examples):
  - [index_concurrently_create_copy](../i/index_concurrently_create_copy.md)
  - [CheckIndexCompatible](../C/CheckIndexCompatible.md)
  - [generateClonedIndexStmt](generateClonedIndexStmt.md)
  - [transformIndexConstraint](../t/transformIndexConstraint.md)
  - [pg_get_indexdef_worker](../p/pg_get_indexdef_worker.md)
  - [RelationGetIndexAttOptions](../R/RelationGetIndexAttOptions.md)

## Notes and Other Information
- The function will throw an ERROR if the specified attribute does not exist in the system cache
- Returns 0 (null datum) when the attribute exists but has no options configured
- The returned datum is a copy, so the caller is responsible for memory management
- Attribute options are stored as text[] arrays and typically contain storage parameters like fillfactor, compression settings, etc.
- This function is commonly used during index operations and DDL command processing where column-specific options need to be preserved or validated

## Simplified Source

```c
Datum get_attoptions(Oid relid, int16 attnum)
{
    HeapTuple tuple;
    Datum attopts;
    bool isnull;

    // Look up the attribute in system cache
    tuple = SearchSysCache2(ATTNUM,
                           ObjectIdGetDatum(relid),
                           Int16GetDatum(attnum));

    // Error if attribute not found
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for attribute %d of relation %u",
             attnum, relid);

    // Extract the attoptions field
    attopts = SysCacheGetAttr(ATTNAME, tuple, Anum_pg_attribute_attoptions,
                             &isnull);

    // Return null datum if no options, otherwise copy the options
    Datum result = isnull ? (Datum) 0 : datumCopy(attopts, false, -1);

    ReleaseSysCache(tuple);
    return result;
}
```