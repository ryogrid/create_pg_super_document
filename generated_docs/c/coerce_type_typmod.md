# coerce_type_typmod

## Location
[src/backend/parser/parse_coerce.c:753-810](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_coerce.c#L753-L810)

## Overview
Forces a value to conform to a particular typmod (type modifier) constraint, handling length and precision coercion for fixed-length data types.

## Definition


## Detailed Description
This function applies typmod constraints to expressions that already have the correct base type. It is commonly used for:

1. **Column Storage**: Applying atttypmod constraints when storing values in relation columns
2. **Explicit Casts**: Enforcing typmod from target type specifications in CAST operations

The function operates by:
- First checking if coercion is needed by comparing current and target typmod values
- For positive typmod values, finding and applying the appropriate coercion function via 
- For negative typmod values or when no coercion function exists, applying a  node to ensure proper typmod exposure
- Optionally hiding input coercion steps for cleaner expression display

The function specifically excludes domain types from processing, as domain typmod coercion is handled during the initial type coercion phase.

## Parameters / Member Variables
- : Input expression node requiring typmod coercion
- : Target type OID (should match the node's current type)
- : Target typmod value to enforce (-1 means no specific constraint)
- : Coercion context affecting semantics of the coercion function
- : Coercion format controlling display properties of generated nodes
- : Parse location for error reporting and expression display
- : If true, forces input node to implicit display form

## Dependencies
- Functions called/Symbols referenced:
  - exprTypmod
  - [hide_coercion_node](../h/hide_coercion_node.md)
  - [find_typmod_coercion_function](../f/find_typmod_coercion_function.md)
  - [build_coercion_expression](../b/build_coercion_expression.md)
  - applyRelabelType
  - [exprCollation](../e/exprCollation.md)
  - [CoercionPathType](../C/CoercionPathType.md) (enum)
  - CoercionContext (enum)
  - CoercionForm (enum)
  - COERCION_PATH_NONE
- Called from (representative examples):
  - [coerce_to_target_type](coerce_to_target_type.md)
  - [coerce_to_domain](coerce_to_domain.md)

## Notes and Other Information
- Declared as static function, only used within parse_coerce.c
- Does not handle domain types - domain typmod is processed during initial type coercion
- Negative typmod values skip functional coercion but still apply RelabelType for proper type exposure
- The hideInputCoercion parameter enables clean expression display by suppressing nested coercion visibility
- Essential for enforcing length constraints on types like VARCHAR(n), CHAR(n), NUMERIC(p,s)
- Always preserves the expression's collation when applying RelabelType
- Located in src/backend/parser/parse_coerce.c:753-810