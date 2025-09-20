# resolve_special_varno

## Location
[src/backend/utils/adt/ruleutils.c:7624-7731](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L7624-L7731)

## Overview
Recursively resolves special variable numbers (OUTER_VAR, INNER_VAR, INDEX_VAR) in plan trees by traversing through nested subplans until finding a concrete expression, then invokes a callback to handle the resolved node.

## Definition

```c
union(context->appendparents,
											   ((Append *) dpns->plan)->apprelids);
```
## Detailed Description
This function implements a recursive resolution mechanism for special variable references that are commonly found in plan trees. Special variables (OUTER_VAR, INNER_VAR, INDEX_VAR) are placeholders that reference expressions from different parts of the execution plan hierarchy.

The function performs the following key operations:
1. Checks if the input node is a Var; if not, immediately invokes the callback
2. For Var nodes, determines the appropriate nesting depth using varlevelsup
3. Handles each special variable type differently:
   - OUTER_VAR: References expressions from the outer relation in joins, with special handling for Append/MergeAppend operations
   - INNER_VAR: References expressions from the inner relation in joins  
   - INDEX_VAR: References expressions from index scans
4. For each special case, retrieves the target list entry, sets up the appropriate child plan context, and recursively calls itself
5. Manages appendparents bitmap for Append/MergeAppend operations to correctly handle inheritance hierarchies
6. Validates variable numbers and reports errors for bogus references

The recursion continues until either a non-Var node is encountered or a regular (non-special) variable number is found, at which point the callback is invoked to handle the final resolved expression.

## Parameters / Member Variables
- : Input node to resolve, typically a Var with special varno
- : Deparse context containing namespace stack and formatting state
- : Function pointer to invoke once resolution is complete
- : Opaque argument passed through to the callback function

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (recursion safety)
  - [list_nth](../l/list_nth.md) (namespace access)
  - [get_tle_by_resno](../g/get_tle_by_resno.md) (target list entry retrieval)
  - [bms_union](../b/bms_union.md) (bitmap set operations for appendparents)
  - [push_child_plan](../p/push_child_plan.md)/pop_child_plan (context management)
  - [resolve_special_varno](resolve_special_varno.md) (recursive self-call)
- Called from (representative examples):
  - [get_variable](../g/get_variable.md)
  - get_agg_expr_helper
  - [resolve_special_varno](resolve_special_varno.md) (recursive calls)

## Notes and Other Information
- Implements stack depth checking to prevent infinite recursion
- Handles inheritance relationships through appendparents bitmap management for Append and MergeAppend plans
- Uses callback pattern to allow different handling strategies for resolved expressions
- Critical component of the plan tree decompilation infrastructure
- The function is tail-recursive in most paths, with the callback invocation as the final step
- Error checking ensures that special variable references have valid target list entries
- Context saving/restoring ensures that nested subplan traversal doesn't affect outer contexts