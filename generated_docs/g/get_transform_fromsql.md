# get_transform_fromsql

## Location
[src/backend/utils/cache/lsyscache.c:2120-2141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2120-L2141)

## Overview
Retrieves the from-SQL transformation function OID for a given data type and procedural language combination, used to convert PostgreSQL data types to language-specific representations.

## Definition
```c
Oid get_transform_fromsql(Oid typid, Oid langid, List *trftypes)
```

## Detailed Description
This function looks up the transformation function that converts data from PostgreSQL's internal SQL representation to a procedural language's native representation. Transforms are used to provide custom conversion logic between PostgreSQL data types and procedural languages like PL/Perl, PL/Python, etc. The function first checks if the requested type is in the list of transformable types, then searches the pg_transform system catalog for a matching transform definition.

The function returns the OID of the from-SQL transformation function if found, or InvalidOid if no transform exists for the specified type-language combination.

## Parameters / Member Variables
- `typid`: The OID of the PostgreSQL data type to be transformed
- `langid`: The OID of the procedural language (e.g., PL/Perl, PL/Python)
- `trftypes`: List of type OIDs for which transforms are available/requested

## Dependencies
- Functions called/Symbols referenced:
  - [list_member_oid](../l/list_member_oid.md) (check if type is in transform list)
  - [SearchSysCache2](../S/SearchSysCache2.md) (system cache lookup with two keys)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (macro to extract structure from tuple)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - Form_pg_transform (pg_transform tuple structure)
- Called from (representative examples):
  - [plperl_ref_from_pg_array](../p/plperl_ref_from_pg_array.md) (PL/Perl)
  - [plperl_call_perl_func](../p/plperl_call_perl_func.md) (PL/Perl)
  - [plperl_hash_from_tuple](../p/plperl_hash_from_tuple.md) (PL/Perl)
  - [PLy_input_setup_func](../P/PLy_input_setup_func.md) (PL/Python)

## Notes and Other Information
- Part of PostgreSQL's transform system that enables custom type conversions for procedural languages
- Returns InvalidOid if the type is not in the transform types list or no transform is defined
- The trffromsql field in pg_transform points to the function that performs the conversion
- Used primarily by procedural language implementations to convert PostgreSQL types to language-native types
- The function performs early validation by checking the trftypes list before accessing the system catalog
- Transform functions enable better integration between PostgreSQL's type system and procedural languages

## Simplified Source

```c
Oid get_transform_fromsql(Oid typid, Oid langid, List *trftypes) {
    // Early check: ensure type is in the transform types list
    if (!list_member_oid(trftypes, typid))
        return InvalidOid;

    // Look up transform definition in system cache
    HeapTuple tup = SearchSysCache2(TRFTYPELANG, ObjectIdGetDatum(typid),
                                    ObjectIdGetDatum(langid));

    if (HeapTupleIsValid(tup)) {
        // Extract the from-SQL transformation function OID
        Oid funcid = ((Form_pg_transform) GETSTRUCT(tup))->trffromsql;
        ReleaseSysCache(tup);
        return funcid;
    } else {
        return InvalidOid;
    }
}
```

This simplified version shows the function's two-step validation and lookup process: first checking if the type is eligible for transformation, then searching the pg_transform catalog for the specific type-language combination to retrieve the from-SQL conversion function.