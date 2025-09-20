# coerce_to_domain

## Location
[src/backend/parser/parse_coerce.c:676-752](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_coerce.c#L676-L752)

## Overview
Creates an expression tree to represent coercion to a domain type, handling domain-specific constraints and base type conversions.

## Definition

```c
structure,
	 * ALTER DOMAIN cannot alter the typtypmod.  But it's unclear that that
	 * would be safe to do anyway, without lots of knowledge about what the
	 * base type thinks the typmod means.
	 */
	arg = coerce_type_typmod(arg, baseTypeId, baseTypeMod,
							 ccontext, COERCE_IMPLICIT_CAST, location,
							 false);
```
## Detailed Description
This function specializes in converting expressions to domain types, which are user-defined types based on existing base types with additional constraints. The function performs a two-phase conversion process:

1. **Base Type Coercion**: First applies any necessary typmod constraints from the domain to the base type using 
2. **Domain Validation**: Creates a  node that represents runtime checking of domain-specific constraints

The function automatically determines the base type and typmod if not provided, and returns the input unchanged if the target type is not actually a domain. It properly handles coercion display formatting and can hide nested coercion steps when requested.

## Parameters / Member Variables
- : Input expression node to be coerced to the domain type
- : Base type OID of the domain (pass InvalidOid if unknown)
- : Base type typmod of the domain (pass -1 if unknown)
- : Target domain type OID to coerce to
- : Coercion context indicating the circumstances of the conversion
- : Coercion format controlling display of the coercion
- : Parse location for error reporting and display
- : If true, suppresses display of nested coercion steps

## Dependencies
- Functions called/Symbols referenced:
  - [getBaseTypeAndTypmod](../g/getBaseTypeAndTypmod.md)
  - [hide_coercion_node](../h/hide_coercion_node.md)
  - [coerce_type_typmod](coerce_type_typmod.md)
  - makeNode
  - CoerceToDomain (node type)
  - CoercionContext (enum)
  - CoercionForm (enum)
  - COERCE_IMPLICIT_CAST
- Called from (representative examples):
  - [coerce_type](coerce_type.md)
  - [coerce_record_to_complex](coerce_record_to_complex.md)
  - [coerce_null_to_domain](coerce_null_to_domain.md)
  - [transformAssignmentIndirection](../t/transformAssignmentIndirection.md)

## Notes and Other Information
- Returns input argument unchanged if the target type is not a domain
- Domain typmod constraints are applied as part of the fixed expression structure, preventing ALTER DOMAIN from modifying typtypmod
- The CoerceToDomain node ensures runtime constraint checking and proper result type labeling
- Currently always sets resulttypmod to -1 for domains
- Uses implicit cast formatting for internal typmod coercion while preserving the original coercion context semantics
- The hideInputCoercion parameter allows suppression of nested coercion display in complex coercion chains
- Located in src/backend/parser/parse_coerce.c:676-752