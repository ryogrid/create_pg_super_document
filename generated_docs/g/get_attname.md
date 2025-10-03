# get_attname

## Location
[src/backend/utils/cache/lsyscache.c:827-857](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L827-L857)

## Overview
Retrieves the attribute name from the PostgreSQL system catalog for a given relation and attribute number, returning it as a palloc'ed string.

## Definition

```c
char *
get_attname(Oid relid, AttrNumber attnum, bool missing_ok)
```
## Detailed Description
This function performs a system cache lookup to retrieve the attribute name (column name) from the pg_attribute catalog table. It searches using the relation OID and attribute number as keys. The function returns a newly allocated string containing the attribute name, which must be freed by the caller. If the attribute is not found, the behavior depends on the missing_ok parameter - it either returns NULL or throws an error.

## Parameters / Member Variables
- `relid`: Object identifier of the relation (table/view/etc.) containing the attribute
- `attnum`: Attribute number (column number) within the relation, typically starting from 1
- `missing_ok`: If true, returns NULL when attribute is not found; if false, throws an error
## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache2](../S/SearchSysCache2.md) - performs the system cache lookup using ATTNUM cache
  - [Int16GetDatum](../I/Int16GetDatum.md) - converts attribute number to Datum format
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) - converts relation OID to Datum format
  - HeapTupleIsValid - checks if the cache lookup returned a valid tuple
  - GETSTRUCT - extracts the Form_pg_attribute structure from the heap tuple
  - NameStr - extracts string from Name type
  - [pstrdup](../p/pstrdup.md) - creates a palloc'ed copy of the string
  - [ReleaseSysCache](../R/ReleaseSysCache.md) - releases the system cache entry
  - elog - logs error messages

- Called from (representative examples):
  - [AddRelationNewConstraints](../A/AddRelationNewConstraints.md) (catalog/heap.c:2495)
  - [getObjectDescription](getObjectDescription.md) (catalog/objectaddress.c:2918)
  - [get_rte_attribute_name](get_rte_attribute_name.md) (parser/parse_relation.c:3272)
  - [pg_get_triggerdef_worker](../p/pg_get_triggerdef_worker.md) (utils/adt/ruleutils.c:971)
  - [errtablecol](../e/errtablecol.md) (utils/cache/relcache.c:5983)

## Notes and Other Information
- Returns a palloc'ed string that must be freed by the caller
- Uses the ATTNUM system cache for efficient lookups
- Part of the PostgreSQL attribute cache subsystem in lsyscache.c
- Essential function for translating internal attribute numbers to user-visible column names
- Commonly used in error reporting, rule deparsing, and object description functions
- The missing_ok parameter allows graceful handling of non-existent attributes in some contexts

## Simplified Source

```c
char *
get_attname(Oid relid, AttrNumber attnum, bool missing_ok)
{
    HeapTuple tp;

    // Look up attribute by relation OID and attribute number
    tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum));
    if (HeapTupleIsValid(tp)) {
        Form_pg_attribute att_tup = (Form_pg_attribute) GETSTRUCT(tp);
        char *result;

        // Extract and copy attribute name
        result = pstrdup(NameStr(att_tup->attname));
        ReleaseSysCache(tp);
        return result;
    }

    // Handle missing attribute
    if (!missing_ok)
        elog(ERROR, "cache lookup failed for attribute %d of relation %u", attnum, relid);
    return NULL;
}
```