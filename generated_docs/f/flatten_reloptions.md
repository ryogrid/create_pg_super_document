# flatten_reloptions

## Location
[src/backend/utils/adt/ruleutils.c:13313-13345](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L13313-L13345)

## Overview
A static utility function that retrieves and formats the reloptions (relation options) for a given relation OID into a C string representation.

## Definition
```c
static char *
flatten_reloptions(Oid relid)
```

## Detailed Description
This function looks up a relation in the system cache by its OID and extracts the reloptions attribute from the pg_class catalog. If reloptions exist for the relation, it uses the `get_reloptions()` function to format them into a comma-separated, properly quoted string suitable for SQL output. The function performs proper error handling for invalid relation OIDs and returns NULL if no reloptions are defined for the relation. This is commonly used when reconstructing DDL statements for relations that have storage parameters or other options.

## Parameters / Member Variables
- `relid`: The OID of the relation whose reloptions should be retrieved and formatted

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - elog
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [initStringInfo](../i/initStringInfo.md)
  - [get_reloptions](../g/get_reloptions.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [pg_get_indexdef_worker](../p/pg_get_indexdef_worker.md)
  - [pg_get_constraintdef_worker](../p/pg_get_constraintdef_worker.md)

## Notes and Other Information
- This is a static function within ruleutils.c, used for SQL object definition reconstruction
- Returns NULL if the relation has no reloptions defined, allowing callers to handle this case appropriately
- Performs proper system cache management with SearchSysCache1/ReleaseSysCache pairing
- Uses error logging (elog) for invalid relation OIDs, which would indicate a serious system inconsistency
- The returned string is allocated in the current memory context and should be managed by the caller
- Accesses the Anum_pg_class_reloptions attribute from the pg_class system catalog

## Simplified Source

```c
static char *
flatten_reloptions(Oid relid)
{
    char *result = NULL;
    HeapTuple tuple;
    Datum reloptions;
    bool isnull;

    // Look up relation in system cache
    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for relation %u", relid);

    // Get reloptions attribute from pg_class
    reloptions = SysCacheGetAttr(RELOID, tuple,
                                Anum_pg_class_reloptions, &isnull);

    // If reloptions exist, format them into a string
    if (!isnull) {
        StringInfoData buf;
        initStringInfo(&buf);
        get_reloptions(&buf, reloptions);
        result = buf.data;
    }

    ReleaseSysCache(tuple);
    return result;
}
```