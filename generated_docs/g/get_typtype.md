# get_typtype

## Location
[src/backend/utils/cache/lsyscache.c:2629-2654](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2629-L2654)

## Overview
Retrieves the type category of a PostgreSQL data type by returning the typtype field from the pg_type system catalog.

## Definition

```c
char
get_typtype(Oid typid)
```
## Detailed Description
This function performs a system catalog lookup to determine the category of a PostgreSQL data type. It returns a single character that indicates the fundamental nature of the type:

- 'b' = base type (built-in types like int4, text, etc.)
- 'c' = composite type (table row types, user-defined composite types)  
- 'd' = domain type (user-defined type based on another type with constraints)
- 'e' = enum type (enumerated type with a fixed set of values)
- 'p' = pseudo-type (polymorphic types like anyelement, anyarray)
- 'r' = range type (types representing ranges of values)
- 'm' = multirange type (arrays of non-overlapping ranges)

The function accesses the pg_type system catalog through the PostgreSQL cache system for efficient lookups. If the type OID is not found in the catalog, it returns the null character ('\0') to indicate failure.

This is a fundamental utility function used throughout PostgreSQL to make type-based decisions in parsing, planning, and execution.

## Parameters / Member Variables
- : OID of the type to look up

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup for pg_type)
  - HeapTupleIsValid (validate tuple existence)
  - GETSTRUCT (extract tuple structure)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (release cache reference)
  - Form_pg_type (pg_type tuple structure)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (convert OID to Datum)

- Called from (representative examples):
  - [CheckAttributeType](../C/CheckAttributeType.md) (src/backend/catalog/heap.c:554)
  - [DefineAggregate](../D/DefineAggregate.md) (src/backend/commands/aggregatecmds.c:338, 379)
  - [CreateCast](../C/CreateCast.md) (src/backend/commands/functioncmds.c:1539, 1540)
  - [DefineType](../D/DefineType.md) (src/backend/commands/typecmds.c:402)
  - [type_is_rowtype](../t/type_is_rowtype.md) (src/backend/utils/cache/lsyscache.c:2659, 2664)
  - [type_is_enum](../t/type_is_enum.md) (src/backend/utils/cache/lsyscache.c:2680)
  - [get_type_func_class](get_type_func_class.md) (src/backend/utils/fmgr/funcapi.c:1332, 1343)

## Notes and Other Information
- Returns '\0' (null character) if the cache lookup fails, which callers should check for
- This is a low-level utility function that forms the basis for many higher-level type classification functions
- The typtype field is a fundamental attribute of every type in PostgreSQL's type system
- Commonly used in conjunction with other type classification functions like type_is_rowtype() and type_is_enum()
- Part of the lsyscache.c module which provides efficient cached access to system catalog information

## Simplified Source

```c
char
get_typtype(Oid typid)
{
    HeapTuple tp;

    // Look up type in system cache
    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));

    if (HeapTupleIsValid(tp)) {
        // Extract type information and get typtype field
        Form_pg_type typtup = (Form_pg_type) GETSTRUCT(tp);
        char result = typtup->typtype;

        ReleaseSysCache(tp);
        return result;
    } else {
        // Return null character if type not found
        return '\0';
    }
}
```