# coerce_type

## Location
[src/backend/parser/parse_coerce.c:157-555](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_coerce.c#L157-L555)

## Overview
Converts an expression from one data type to a different type, implementing the core type coercion logic in PostgreSQL's parser system.

## Definition

```c
enum, range, or multirange
		 * type.  In particular the argument must *not* be an UNKNOWN
		 * constant.  If it is, we just fall through;
```
## Detailed Description
This function performs the fundamental type conversion operations in PostgreSQL, handling various coercion scenarios:

1. **Identity Cases**: Returns the node unchanged when no conversion is needed (same types) or when dealing with polymorphic pseudotypes
2. **Unknown Constants**: Converts UNKNOWN string literals by applying the target type's input function
3. **Parameter Coercion**: Delegates to custom parameter coercion hooks when available
4. **CollateExpr Handling**: Manages COLLATE clauses by pushing coercion underneath or discarding for non-collatable types
5. **Standard Coercion**: Uses the coercion pathway system to find and apply appropriate conversion functions
6. **Complex Type Coercion**: Handles RECORD and complex array type conversions
7. **Inheritance Coercion**: Manages subclass to superclass conversions using ConvertRowtypeExpr

The function assumes the caller has already verified the coercion is possible via . It focuses on type conversion only; typmod (length) constraints are typically handled separately by .

## Parameters / Member Variables
- : Parse state context (can be NULL if parameter type resolution is not needed)
- : Input expression tree to be converted
- : Current type OID of the input expression
- : Desired target type OID for conversion
- : Target typmod (usually -1, as length coercion is handled separately)
- : Coercion context indicating the circumstances of the conversion
- : Coercion format controlling display and behavior of the coercion
- : Parse location for error reporting, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - [find_coercion_pathway](../f/find_coercion_pathway.md)
  - [build_coercion_expression](../b/build_coercion_expression.md)
  - [coerce_to_domain](coerce_to_domain.md)
  - [coerce_record_to_complex](coerce_record_to_complex.md)
  - [getBaseType](../g/getBaseType.md)
  - [getBaseTypeAndTypmod](../g/getBaseTypeAndTypmod.md)
  - [typeInheritsFrom](../t/typeInheritsFrom.md)
  - [stringTypeDatum](../s/stringTypeDatum.md)
  - [makeRelabelType](../m/makeRelabelType.md)
  - [type_is_collatable](../t/type_is_collatable.md)
- Called from (representative examples):
  - [coerce_to_target_type](coerce_to_target_type.md)
  - transformArrayExpr
  - [make_fn_arguments](../m/make_fn_arguments.md)
  - [coerce_to_common_type](coerce_to_common_type.md)
  - [buildMergedJoinVar](../b/buildMergedJoinVar.md)

## Notes and Other Information
- Must not modify the input expression tree; only adds decoration on top
- Special handling for INTERVAL type which requires typmod to be passed to input function
- Contains extensive logic for domain type handling, ensuring base type operations are performed first
- Includes debug code for detecting unstable input functions when RANDOMIZE_ALLOCATED_MEMORY is enabled
- Uses recursive calls for CollateExpr processing
- Implements inheritance-based coercion using ConvertRowtypeExpr for complex type conversions
- Located in src/backend/parser/parse_coerce.c:157-555