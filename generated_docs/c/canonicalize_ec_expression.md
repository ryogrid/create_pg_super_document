# canonicalize_ec_expression

## Location
src/backend/optimizer/path/equivclass.c: 471 - 515

## Overview
Ensures that expressions in EquivalenceClasses expose the correct data type and collation for proper equality comparisons and consistent behavior across equivalent expressions.

## Definition


## Detailed Description
This function standardizes expressions before they are stored in EquivalenceClasses to ensure that equivalent expressions can be properly matched using equal() comparisons. It addresses two main issues:

1. **Type Canonicalization**: Expressions from different sources (quals, index keys, sort expressions) may have different exposed types even when they're logically equivalent. The function ensures the exposed type matches what would be expected for the EC's operator families.

2. **Collation Standardization**: In constructs like "foo < bar COLLATE baz", only one expression may have the correct exposed collation from the parser. This function ensures both expressions expose the same collation.

The function handles polymorphic and RECORD types specially by preserving the original expression's type rather than imposing a specific type. When type changes are needed, it sets typmod to -1 since the new type may have different typmod interpretation. For collation-only changes, the original typmod is preserved.

The function uses applyRelabelType to preserve const-flatness, which is crucial since const expressions have already been processed by eval_const_expressions.

## Parameters / Member Variables
- : Input expression to be canonicalized
- : Required data type that the expression should expose
- : Required collation that the expression should expose

## Dependencies
- Functions called/Symbols referenced:
  - exprType
  - exprCollation
  - exprTypmod
  - IsPolymorphicType
  - applyRelabelType
- Called from (representative examples):
  - process_equivalence
  - get_eclass_for_sort_expr
  - convert_subquery_pathkeys

## Notes and Other Information
- Preserves const-flatness by using applyRelabelType rather than direct RelabelType construction
- Handles polymorphic and RECORD types by preserving the original expression type
- Critical for ensuring that expressions from different sources (index keys, sort expressions, quals) can be properly matched in EquivalenceClasses
- Returns the original expression unchanged if no type or collation adjustment is needed