# get_typmodout

## Location
[src/backend/utils/cache/lsyscache.c:3031-3055](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L3031-L3055)

## Overview
Retrieves the type modifier output function (typmodout) OID for a given PostgreSQL data type, used for formatting type modifier values into human-readable string representations.

## Definition
```c
Oid get_typmodout(Oid typid)
```

## Detailed Description
This function looks up the type modifier output function for a specified type from the PostgreSQL system catalog. Type modifier output functions are responsible for converting internal type modifier values (stored as int32) back into human-readable string representations for display purposes. The function searches the pg_type system table and returns the OID of the typmodout function, or InvalidOid if the type doesn't have one or if the type doesn't exist.

Like its counterpart get_typmodin, this function gracefully handles invalid type OIDs by returning InvalidOid rather than throwing an error, making it suitable for optional type modifier processing scenarios.

## Parameters / Member Variables
- `typid`: The OID of the PostgreSQL data type for which the type modifier output function is needed

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
- Returns InvalidOid if the type OID is not found in the system catalog, providing graceful error handling
- Type modifier output functions are optional - many types don't have them and will return InvalidOid
- Type modifiers are used for types that accept parameters, such as NUMERIC(precision, scale), VARCHAR(length), etc.
- The typmodout function takes an int32 typmod value and converts it back to a string representation for display
- Part of the type modifier system infrastructure along with get_typmodin
- Complements get_typmodin by providing the reverse transformation (int32 typmod → string representation)
- Not currently declared in public headers, suggesting it may be intended for internal use only

## Simplified Source

```c
Oid get_typmodout(Oid typid) {
    // Look up type in system cache
    HeapTuple tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));

    if (HeapTupleIsValid(tp)) {
        // Extract pg_type struct and get typmodout function OID
        Form_pg_type typtup = (Form_pg_type) GETSTRUCT(tp);
        Oid result = typtup->typmodout;

        // Clean up cache reference
        ReleaseSysCache(tp);
        return result;
    } else {
        // Return invalid OID if type not found
        return InvalidOid;
    }
}
```