# polymorphic_actuals

## Location
src/backend/utils/fmgr/funcapi.c: 35 - 41

## Overview
A structure that stores the resolved actual types for PostgreSQL's polymorphic pseudo-types during function argument resolution.

## Definition


## Detailed Description
The  structure is a fundamental data type used in PostgreSQL's function manager (fmgr) system for resolving polymorphic pseudo-types. PostgreSQL supports several polymorphic pseudo-types (anyelement, anyarray, anyrange, anymultirange) that allow functions to work with multiple data types. This structure stores the actual resolved OIDs (Object Identifiers) for these pseudo-types during function call resolution.

When a function is called with polymorphic arguments, the system must determine what actual types the pseudo-types represent based on the provided arguments. The  structure acts as a cache or registry to hold these resolved type mappings, ensuring consistency across all polymorphic parameters in a function call.

## Parameters / Member Variables
- : OID of the resolved type for the  pseudo-type, representing any single data type
- : OID of the resolved type for the  pseudo-type, representing any array type
- : OID of the resolved type for the  pseudo-type, representing any range type
- : OID of the resolved type for the  pseudo-type, representing any multirange type

## Dependencies
- Functions called/Symbols referenced:
  - (This is a data structure with no direct function calls)
- Called from (representative examples):
  - resolve_anyelement_from_others
  - resolve_anyarray_from_others  
  - resolve_anyrange_from_others
  - resolve_anymultirange_from_others
  - resolve_polymorphic_tupdesc
  - resolve_polymorphic_argtypes

## Notes and Other Information
- Located in 
- This structure is central to PostgreSQL's polymorphic type system, which allows writing generic functions that work with multiple data types
- The structure uses PostgreSQL's OID system to identify types, where InvalidOid typically indicates an unresolved or unknown type
- The polymorphic type resolution process ensures type consistency across all parameters in a function call - for example, if one parameter resolves  to , all other  parameters in the same function call must also be 
- This mechanism is essential for PostgreSQL's extensibility, allowing both built-in and user-defined functions to work generically with various data types