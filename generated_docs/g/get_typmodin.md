# get_typmodin

## Location
[src/backend/utils/cache/lsyscache.c:3006-3030](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L3006-L3030)

## Overview
Retrieves the type modifier input function (typmodin) OID for a given PostgreSQL data type, used for parsing and validating type modifier specifications.

## Definition
```c
Oid get_typmodin(Oid typid)
```

## Detailed Description
This function looks up the type modifier input function for a specified type from the PostgreSQL system catalog. Type modifier input functions are responsible for parsing and validating type modifier specifications (like precision and scale for numeric types, or length for varchar). The function searches the pg_type system table and returns the OID of the typmodin function, or InvalidOid if the type doesn't have one or if the type doesn't exist.

Unlike some other type lookup functions, this function gracefully handles invalid type OIDs by returning InvalidOid rather than throwing an error, making it suitable for optional type modifier processing scenarios.

## Parameters / Member Variables
- `typid`: The OID of the PostgreSQL data type for which the type modifier input function is needed

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system catalog cache lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (extract struct from heap tuple)
  - Form_pg_type (pg_type catalog structure)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (release cache reference)
- Called from (representative examples):
  - Currently not directly referenced in the analyzed codebase (may be used via function pointers or external modules)

## Notes and Other Information
- Returns InvalidOid if the type OID is not found in the system catalog, unlike other similar functions that throw errors
- Type modifier input functions are optional - many types don't have them and will return InvalidOid
- Type modifiers are used for types that accept parameters, such as NUMERIC(precision, scale), VARCHAR(length), etc.
- The typmodin function takes an array of type modifier strings and converts them to a single int32 typmod value
- Part of the type modifier system infrastructure along with get_typmodout
- Declared in src/include/utils/lsyscache.h as part of the public API

## Simplified Source

```c
Oid get_typmodin(Oid typid) {
    // Look up type in system cache
    HeapTuple tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));

    if (HeapTupleIsValid(tp)) {
        // Extract pg_type struct and get typmodin function OID
        Form_pg_type typtup = (Form_pg_type) GETSTRUCT(tp);
        Oid result = typtup->typmodin;

        // Clean up cache reference
        ReleaseSysCache(tp);
        return result;
    } else {
        // Return invalid OID if type not found
        return InvalidOid;
    }
}
```