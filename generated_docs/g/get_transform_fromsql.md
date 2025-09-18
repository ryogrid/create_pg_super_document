# get_transform_fromsql

## Location
src/backend/utils/cache/lsyscache.c: 2120 - 2141

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
  - list_member_oid (check if type is in transform list)
  - SearchSysCache2 (system cache lookup with two keys)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (macro to extract structure from tuple)
  - ReleaseSysCache (cache cleanup)
  - Form_pg_transform (pg_transform tuple structure)
- Called from (representative examples):
  - plperl_ref_from_pg_array (PL/Perl)
  - plperl_call_perl_func (PL/Perl)
  - plperl_hash_from_tuple (PL/Perl)
  - PLy_input_setup_func (PL/Python)

## Notes and Other Information
- Part of PostgreSQL's transform system that enables custom type conversions for procedural languages
- Returns InvalidOid if the type is not in the transform types list or no transform is defined
- The trffromsql field in pg_transform points to the function that performs the conversion
- Used primarily by procedural language implementations to convert PostgreSQL types to language-native types
- The function performs early validation by checking the trftypes list before accessing the system catalog
- Transform functions enable better integration between PostgreSQL's type system and procedural languages