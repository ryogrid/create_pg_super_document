# scalararraysel

## Location
[src/backend/utils/adt/selfuncs.c:1817-2139](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L1817-L2139)

## Overview
Computes the selectivity of ScalarArrayOpExpr nodes, handling SQL operations like 'value = ANY(array)' and 'value <> ALL(array)' with sophisticated array analysis.

## Definition

```c
struct the expression */
	Assert(list_length(clause->args) == 2);
```
## Detailed Description
The  function estimates selectivity for scalar array operations, which are SQL expressions comparing a scalar value against an array using operators like ANY or ALL. Examples include 'column = ANY(ARRAY[1,2,3])' or 'value <> ALL(array_column)'.

The function implements a sophisticated multi-tiered approach:

1. **Array Containment Optimization**: For equality/inequality operations, it first attempts to use array containment analysis via , treating expressions like 'const = ANY(column)' as 'ARRAY[const] <@ column' for more accurate estimates.

2. **Constant Array Analysis**: When the array is a constant, it deconstructs the array elements and applies the operator's selectivity function to each element. It uses two probability models:
   - **Independent probabilities**: Standard assumption for generic operators
   - **Disjoint probabilities**: For equality/inequality with distinct elements, probabilities are summed rather than combined independently

3. **ArrayExpr Analysis**: When the array is constructed using ARRAY[] syntax, it processes each element expression individually, applying similar probability combination logic.

4. **Fallback Estimation**: When the array structure is unknown, it creates a dummy element and assumes approximately 10 elements in the array for estimation purposes.

The function handles both OR semantics (ANY operations) and AND semantics (ALL operations), with appropriate probability combination formulas for each case.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information
- : ScalarArrayOpExpr node representing the scalar-array operation
- : Boolean indicating if this is part of a join condition
- : Relation ID to restrict analysis to (0 if no restriction)
- : Type of join operation context
- : Special join information for outer joins

## Dependencies
- Functions called/Symbols referenced:
  - estimate_expression_value
  - get_base_element_type
  - strip_array_coercion
  - scalararraysel_containment
  - lookup_type_cache
  - get_oprjoin/get_oprrest
  - deconstruct_array
  - CLAMP_PROBABILITY
- Called from (representative examples):
  - clause_selectivity_ext
  - GenericCosts

## Notes and Other Information
- Handles both ANY (OR) and ALL (AND) array operations with appropriate probability mathematics
- Uses sophisticated disjoint probability analysis for equality operations with distinct array elements
- Preprocesses expressions to remove binary-compatible type coercions using strip_array_coercion
- Falls back through multiple analysis strategies based on array expression complexity
- Critical for optimizing queries with IN clauses and array operations
- Assumes 10 elements for unknown array sizes (also used in estimate_array_length)
- Supports both constant arrays and dynamic ARRAY[] constructs
- Ensures final selectivity values are clamped to valid probability range [0.0, 1.0]