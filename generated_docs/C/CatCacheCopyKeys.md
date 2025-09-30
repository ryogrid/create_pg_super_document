# CatCacheCopyKeys

## Location
[src/backend/utils/cache/catcache.c:2286-2355](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L2286-L2355)

## Overview
CatCacheCopyKeys is a static helper function that safely copies catalog cache keys from a source array to a destination array, ensuring all datums are properly allocated in the current memory context.

## Definition
```c
static void CatCacheCopyKeys(TupleDesc tupdesc, int nkeys, int *attnos,
                            Datum *srckeys, Datum *dstkeys)
```

## Detailed Description
This function performs a deep copy of catalog cache keys, which is essential for maintaining proper memory management in PostgreSQL's catalog cache system. The function iterates through each key and uses datumCopy() to ensure that variable-length data types are properly allocated in the current memory context rather than potentially referencing freed memory.

A special case is handled for NAME type attributes (NAMEOID), where C strings are converted to properly padded NAME values using namestrcpy() to prevent potential memory access violations during the copy operation.

## Parameters / Member Variables
- `tupdesc`: Tuple descriptor containing attribute information for the keys
- `nkeys`: Number of keys to copy
- `attnos`: Array of attribute numbers for each key
- `srckeys`: Source array of Datum values to copy from
- `dstkeys`: Destination array of Datum values to copy to

## Dependencies
- Functions called/Symbols referenced:
  - TupleDescAttr (macro to get attribute from tuple descriptor)
  - [namestrcpy](../n/namestrcpy.md) (converts C string to padded NAME)
  - [DatumGetCString](../D/DatumGetCString.md) (extracts C string from Datum)
  - [NameGetDatum](../N/NameGetDatum.md) (converts NAME to Datum)
  - [datumCopy](../d/datumCopy.md) (performs deep copy of datum)
- Called from (representative examples):
  - [SearchCatCacheList](../S/SearchCatCacheList.md) (src/backend/utils/cache/catcache.c:1988)
  - [CatalogCacheCreateEntry](CatalogCacheCreateEntry.md) (src/backend/utils/cache/catcache.c:2225)

## Notes and Other Information
- This is a static function, only accessible within catcache.c
- Contains a performance optimization note suggesting that memory and lookup performance could be improved by storing all keys in one allocation
- Special handling for NAMEOID prevents buffer overruns when copying NAME type data
- Essential for proper memory management in catalog cache operations

## Simplified Source

```c
static void
CatCacheCopyKeys(TupleDesc tupdesc, int nkeys, int *attnos,
                 Datum *srckeys, Datum *dstkeys)
{
    int i;

    // Copy each key, ensuring proper memory allocation
    for (i = 0; i < nkeys; i++) {
        int attnum = attnos[i];
        Form_pg_attribute att = TupleDescAttr(tupdesc, attnum - 1);
        Datum src = srckeys[i];
        NameData srcname;

        // Special case: convert C string to properly padded NAME
        if (att->atttypid == NAMEOID) {
            namestrcpy(&srcname, DatumGetCString(src));
            src = NameGetDatum(&srcname);
        }

        // Perform deep copy using attribute properties
        dstkeys[i] = datumCopy(src, att->attbyval, att->attlen);
    }
}
```