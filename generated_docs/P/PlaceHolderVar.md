# PlaceHolderVar

## Location
[src/include/nodes/pathnodes.h:2780-2800](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L2780-L2800)

## Overview
PlaceHolderVar is a placeholder node for expressions that need to be evaluated below the top level of a plan tree, typically used during planning to represent expressions that may yield NULL values when evaluated above an outer join.

## Definition


## Detailed Description
PlaceHolderVar represents an expression that must be evaluated at a specific level in the plan tree, typically below an outer join. During planning, it serves as a placeholder for expressions that might yield NULL instead of their actual value when referenced above outer joins. At the end of planning, PlaceHolderVars are replaced either by the contained expression itself or by a Var that refers to a lower-level evaluation of the expression.

The structure is designed with specific comparison semantics - two PlaceHolderVars with the same ID and levelsup are considered equal even if their contained expressions differ, which can happen during plan construction when nested PHVs are processed or when initplan sublinks get replaced.

## Parameters / Member Variables
- : Base expression node structure
- : The actual expression being represented by this placeholder (ignored in equality comparisons)
- : Set of relation IDs where this placeholder is syntactically valid (ignored in equality comparisons)
- : Set of outer join RT indexes that can cause this PHV's value to become NULL
- : Unique identifier for this PHV within the current planner run
- : Nesting level indicator, > 0 if PHV belongs to an outer query level

## Dependencies
- Functions called/Symbols referenced:
  - [Expr](../E/Expr.md) (base expression type)
  - Relids (relation ID sets)
  - Index (identifier type)

- Called from (representative examples):
  - [make_placeholder_expr](../m/make_placeholder_expr.md)
  - [find_placeholder_info](../f/find_placeholder_info.md)
  - [replace_nestloop_params_mutator](../r/replace_nestloop_params_mutator.md)
  - [fix_scan_expr_mutator](../f/fix_scan_expr_mutator.md)
  - [pullup_replace_vars_callback](../p/pullup_replace_vars_callback.md)

## Notes and Other Information
- Not recognized by parser or executor, only used during planning phase
- Declared in pathnodes.h rather than primnodes.h due to its planner-specific nature
- Equality comparison intentionally ignores phexpr and phrels fields to handle plan construction complexities
- Critical for handling expressions that cross outer join boundaries where NULL semantics matter
- Used extensively in join planning and parameter assignment operations