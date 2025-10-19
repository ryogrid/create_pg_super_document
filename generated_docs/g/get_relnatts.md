# get_relnatts

## Location
[src/backend/utils/cache/lsyscache.c:1899-1927](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L1899-L1927)

## Overview
Returns the number of attributes (columns) for a given relation, or InvalidAttrNumber if the relation is not found.

## Definition
```c
int get_relnatts(Oid relid)
```

## Detailed Description
This function retrieves the number of attributes (columns) for a PostgreSQL relation from the system catalog. It performs a system cache lookup on the pg_class catalog to retrieve the relnatts field, which contains the count of user-visible columns in the relation. This information is fundamental for many operations that need to know the structure of a table or relation, such as tuple processing, query planning, and schema validation.

## Parameters / Member Variables
- `relid`: The OID of the relation for which to retrieve the attribute count

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - Form_pg_class
  - InvalidAttrNumber
- Called from (representative examples):
  - Currently no references found in the analyzed codebase

## Notes and Other Information
- Returns InvalidAttrNumber if the relation OID is not found in the system cache
- The returned count includes only user-visible attributes, not system columns
- This function is part of the relation cache subsystem for efficient metadata access
- Unlike some other cache functions, this one gracefully handles missing relations by returning InvalidAttrNumber rather than raising an error
- The relnatts field in pg_class represents the number of user attributes in the relation

## Simplified Source

```c
int get_relnatts(Oid relid) {
    // Look up relation in system cache
    HeapTuple tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));

    if (HeapTupleIsValid(tp)) {
        // Extract pg_class struct and get attribute count
        Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);
        int result = reltup->relnatts;

        // Clean up cache reference
        ReleaseSysCache(tp);
        return result;
    } else {
        // Return invalid marker if relation not found
        return InvalidAttrNumber;
    }
}
```