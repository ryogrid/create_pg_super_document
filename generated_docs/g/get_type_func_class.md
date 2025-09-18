# get_type_func_class

## Location
[src/backend/utils/fmgr/funcapi.c:1328-1378](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/funcapi.c#L1328-L1378)

## Overview
Classifies a PostgreSQL data type by its functional characteristics and returns the base type OID if it's a domain type.

## Definition


## Detailed Description
This function centralizes the logic for classifying PostgreSQL data types into functional categories that determine how functions returning those datatypes should be handled. It examines the type's characteristics and returns a TypeFuncClass enumeration value indicating the appropriate handling strategy.

The function handles domain types specially by resolving them to their base types and adjusting the classification accordingly. For domain types over composite types, it returns a special COMPOSITE_DOMAIN classification to distinguish them from regular composite types.

The classification is particularly important for determining return value handling in function calls, especially for functions that return complex types like records or composite types.

## Parameters / Member Variables
- : The OID of the type to classify
- : Output parameter that receives the base type OID (same as typid unless it's a domain)

## Dependencies
- Functions called/Symbols referenced:
  - [get_typtype](get_typtype.md)
  - [getBaseType](getBaseType.md)
  - TYPTYPE_COMPOSITE/BASE/ENUM/RANGE/MULTIRANGE/DOMAIN/PSEUDO constants
  - TYPEFUNC_COMPOSITE/SCALAR/COMPOSITE_DOMAIN/RECORD/OTHER constants
  - RECORDOID, VOIDOID, CSTRINGOID constants
- Called from (representative examples):
  - [get_expr_result_type](get_expr_result_type.md)
  - [internal_get_result_type](../i/internal_get_result_type.md)
  - [TypeGetTupleDesc](../T/TypeGetTupleDesc.md)

## Notes and Other Information
- Returns TypeFuncClass enum values: TYPEFUNC_SCALAR, TYPEFUNC_COMPOSITE, TYPEFUNC_COMPOSITE_DOMAIN, TYPEFUNC_RECORD, or TYPEFUNC_OTHER
- Special handling for VOID and CSTRING pseudo-types: treats them as legitimate scalar datatypes for JDBC driver convenience
- Domain types are resolved to their base types, with special classification if the base is composite
- The function is static (internal to funcapi.c) and serves as a utility for other type-handling functions
- Critical for proper function return value processing in PostgreSQL's type system