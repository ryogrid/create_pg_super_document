# AggregateCreate

## Location
[src/backend/catalog/pg_aggregate.c:46-825](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_aggregate.c#L46-L825)

## Overview
AggregateCreate is the core function responsible for creating new aggregate functions in PostgreSQL, validating their definitions, and inserting them into the system catalogs.

## Definition


## Detailed Description
AggregateCreate is the central function for creating aggregate functions in PostgreSQL. It performs comprehensive validation of all aggregate components including transition functions, final functions, combine functions, and serialization/deserialization functions. The function handles different aggregate types (normal, ordered-set, hypothetical-set) and supports both single-phase and moving-aggregate implementations.

The function validates polymorphic types, ensures proper function signatures match expected patterns, checks permissions on all referenced types and functions, and creates entries in both pg_proc and pg_aggregate system catalogs. It also establishes dependency relationships between the aggregate and all its component functions.

## Parameters / Member Variables
- : Name of the aggregate function being created
- : Namespace (schema) OID where the aggregate will be created
- : Whether to replace an existing aggregate with the same signature
- : Type of aggregate (normal, ordered-set, or hypothetical-set)
- : Total number of aggregate arguments
- : Number of direct arguments (for ordered-set aggregates)
- : Vector of parameter type OIDs
- : All parameter types including OUT parameters
- : Parameter modes (IN, OUT, INOUT, VARIADIC)
- : Parameter names
- : Default values for parameters
- : Type OID for variadic arguments, if any
- : Name of the state transition function
- : Name of the final function (optional)
- : Name of the combine function for parallel aggregation (optional)
- : Name of the serialization function (optional)
- : Name of the deserialization function (optional)
- : Name of the forward transition function for moving aggregates (optional)
- : Name of the inverse transition function for moving aggregates (optional)
- : Name of the final function for moving aggregates (optional)
- : Whether final function receives extra arguments
- : Whether moving-aggregate final function receives extra arguments
- : Whether final function modifies transition state
- : Whether moving-aggregate final function modifies transition state
- : Name of the sort operator for ordered-set aggregates (optional)
- : OID of the state transition data type
- : Estimated average size of transition state
- : OID of the moving-aggregate transition data type (optional)
- : Estimated average size of moving-aggregate transition state
- : Initial value for transition state (optional)
- : Initial value for moving-aggregate transition state (optional)
- : Parallel safety level of the aggregate

## Dependencies
- Functions called/Symbols referenced:
  - [lookup_agg_function](../l/lookup_agg_function.md): Validates and finds component functions
  - [check_valid_polymorphic_signature](../c/check_valid_polymorphic_signature.md): Validates polymorphic type signatures
  - [ProcedureCreate](../P/ProcedureCreate.md): Creates the pg_proc entry for the aggregate
  - [IsBinaryCoercible](../I/IsBinaryCoercible.md): Checks type compatibility
  - [LookupOperName](../L/LookupOperName.md): Finds sort operators for ordered-set aggregates
  - [object_aclcheck](../o/object_aclcheck.md): Checks permissions on types and functions
  - [record_object_address_dependencies](../r/record_object_address_dependencies.md): Establishes dependency relationships

- Called from (representative examples):
  - [DefineAggregate](../D/DefineAggregate.md): Main entry point from CREATE AGGREGATE command

## Notes and Other Information
The function performs extensive validation including checking that transition function return types match declared transition types, ensuring polymorphic consistency across all components, and validating that moving-aggregate implementations produce the same result type as regular implementations. It supports parallel aggregation through combine functions and window functions through moving-aggregate implementations with forward/inverse transition functions.

The function handles replacement of existing aggregates carefully, ensuring that critical properties like aggregate kind and number of direct arguments cannot be changed, as these would break existing callers.