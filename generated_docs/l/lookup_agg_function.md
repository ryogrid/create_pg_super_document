# lookup_agg_function

## Location
[src/backend/catalog/pg_aggregate.c:826-914](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_aggregate.c#L826-L914)

## Overview
lookup_agg_function is a utility function that validates and resolves function names for aggregate support functions, ensuring they meet the requirements for use in aggregate definitions.

## Definition


## Detailed Description
lookup_agg_function serves as the common validation and resolution function for all aggregate support functions (transition functions, final functions, combine functions, etc.). It performs function lookup using the system catalog, handles polymorphic type resolution, ensures type consistency, validates that no runtime type coercion is required, and checks permissions.

The function uses func_get_detail to resolve the function name and handle polymorphic types, then performs additional validation specific to aggregate functions. It ensures the function doesn't return a set, handles VARIADIC ANY consistency, validates that no runtime type coercion will be needed, and verifies the caller has execute permissions on the function.

## Parameters / Member Variables
- : List representing the possibly schema-qualified function name to lookup
- : Number of expected function arguments
- : Array of expected argument type OIDs (must not be modified)
- : OID of variadic argument type if any, InvalidOid otherwise
- : Pointer to store the resolved return type OID of the function

## Dependencies
- Functions called/Symbols referenced:
  - [func_get_detail](../f/func_get_detail.md): Core function lookup and polymorphic resolution
  - [func_signature_string](../f/func_signature_string.md): Creates function signature strings for error messages
  - [enforce_generic_type_consistency](../e/enforce_generic_type_consistency.md): Ensures polymorphic type consistency
  - [IsBinaryCoercible](../I/IsBinaryCoercible.md): Checks if types are binary compatible without coercion
  - [object_aclcheck](../o/object_aclcheck.md): Verifies execute permissions on the function
  - [aclcheck_error](../a/aclcheck_error.md): Reports permission errors
  - [get_func_name](../g/get_func_name.md): Gets function name for error reporting

- Called from (representative examples):
  - [AggregateCreate](../A/AggregateCreate.md): Used 8 times to validate different aggregate support functions (transition, final, combine, serial/deserial, moving-aggregate functions)

## Notes and Other Information
This function is critical for aggregate validation because it ensures that all component functions can be called efficiently without runtime type coercion, which is essential for aggregate performance. The function is particularly careful about VARIADIC ANY consistency and polymorphic type resolution, as these are common sources of aggregate definition errors.

The function enforces that aggregate support functions cannot return sets, as this would be meaningless in the context of aggregation. It also validates that the caller has execute permissions on all referenced functions, ensuring proper security in aggregate definitions.