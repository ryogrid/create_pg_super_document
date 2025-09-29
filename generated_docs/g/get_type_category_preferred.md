# get_type_category_preferred

## Location
[src/backend/utils/cache/lsyscache.c:2710-2730](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2710-L2730)

## Overview
A system cache utility function that retrieves both the category and preferred-type status for a given PostgreSQL type OID.

## Definition
```c
void get_type_category_preferred(Oid typid, char *typcategory, bool *typispreferred)
```

## Detailed Description
This function performs a system catalog lookup to fetch two important type attributes from the pg_type system catalog: the type category and whether the type is marked as preferred within its category. Type categories group related types together (e.g., numeric types, string types), and preferred types are used by the type resolution system to make decisions when multiple candidate types are available. The function throws an error if the type OID is not found in the system catalog.

## Parameters / Member Variables
- `typid`: The OID (object identifier) of the PostgreSQL type to look up
- `typcategory`: Output parameter that receives the type's category character
- `typispreferred`: Output parameter that receives whether the type is preferred in its category

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - Form_pg_type
- Called from (representative examples):
  - [select_common_type](../s/select_common_type.md)
  - [select_common_type_from_oids](../s/select_common_type_from_oids.md)
  - [TypeCategory](../T/TypeCategory.md)
  - [IsPreferredType](../I/IsPreferredType.md)
  - [transformJsonValueExpr](../t/transformJsonValueExpr.md)
  - [func_select_candidate](../f/func_select_candidate.md)

## Notes and Other Information
This function is a core component of PostgreSQL's type system infrastructure. It's heavily used in type resolution algorithms, particularly in contexts where the parser needs to determine which type to use when multiple options are available (such as in function overload resolution or UNION operations). The function accesses the system cache for efficiency, as type information is frequently queried during query planning and execution.

## Simplified Source

```c
void
get_type_category_preferred(Oid typid, char *typcategory, bool *typispreferred)
{
    HeapTuple tp;
    Form_pg_type typtup;

    // Look up type in system cache
    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (!HeapTupleIsValid(tp))
        elog(ERROR, "cache lookup failed for type %u", typid);

    // Extract type information from tuple
    typtup = (Form_pg_type) GETSTRUCT(tp);
    *typcategory = typtup->typcategory;
    *typispreferred = typtup->typispreferred;

    // Release cache entry
    ReleaseSysCache(tp);
}
```