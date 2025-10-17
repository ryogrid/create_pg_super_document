# get_transform_tosql

## Location
[src/backend/utils/cache/lsyscache.c:2142-2172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2142-L2172)

## Overview
Retrieves the to-SQL transformation function OID for a given data type and procedural language combination, used to convert language-specific representations back to PostgreSQL data types.

## Definition
```c
Oid get_transform_tosql(Oid typid, Oid langid, List *trftypes)
```

## Detailed Description
This function looks up the transformation function that converts data from a procedural language's native representation back to PostgreSQL's internal SQL representation. This is the complementary function to get_transform_fromsql, enabling bidirectional conversion between PostgreSQL data types and procedural languages like PL/Perl, PL/Python, etc. The function first checks if the requested type is in the list of transformable types, then searches the pg_transform system catalog for a matching transform definition.

The function returns the OID of the to-SQL transformation function if found, or InvalidOid if no transform exists for the specified type-language combination.

## Parameters / Member Variables
- `typid`: The OID of the PostgreSQL data type to be transformed to
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
  - [plperl_sv_to_datum](../p/plperl_sv_to_datum.md) (PL/Perl)
  - [PLy_output_setup_func](../P/PLy_output_setup_func.md) (PL/Python)

## Notes and Other Information
- Part of PostgreSQL's transform system that enables custom type conversions for procedural languages
- Returns InvalidOid if the type is not in the transform types list or no transform is defined
- The trftosql field in pg_transform points to the function that performs the conversion
- Used primarily by procedural language implementations to convert language-native types back to PostgreSQL types
- The function performs early validation by checking the trftypes list before accessing the system catalog
- Complements get_transform_fromsql to provide bidirectional type conversion capabilities
- Transform functions enable seamless data exchange between PostgreSQL's type system and procedural languages

## Simplified Source

```c
Oid get_transform_tosql(Oid typid, Oid langid, List *trftypes) {
    // Early check: ensure type is in the transform types list
    if (!list_member_oid(trftypes, typid))
        return InvalidOid;

    // Look up transform definition in system cache
    HeapTuple tup = SearchSysCache2(TRFTYPELANG, ObjectIdGetDatum(typid),
                                    ObjectIdGetDatum(langid));

    if (HeapTupleIsValid(tup)) {
        // Extract the to-SQL transformation function OID
        Oid funcid = ((Form_pg_transform) GETSTRUCT(tup))->trftosql;
        ReleaseSysCache(tup);
        return funcid;
    } else {
        return InvalidOid;
    }
}
```

This simplified version shows the function's identical structure to get_transform_fromsql but retrieving the complementary to-SQL transformation function. It validates the type is transformable, searches the pg_transform catalog, and returns the function that converts from language-native format back to PostgreSQL's SQL format.