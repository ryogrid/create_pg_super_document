# map_variable_attnos_mutator

## Location
[src/backend/rewrite/rewriteManip.c:1492-1614](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L1492-L1614)

## Overview
The internal mutator function that performs recursive tree walking to remap column attribute numbers in Var nodes, handling whole-row variables and type conversions as needed.

## Definition

```c
static Node *
map_variable_attnos_mutator(Node *node,
							map_variable_attnos_context *context)
```
## Detailed Description
This function implements the core logic for attribute number remapping in PostgreSQL expression trees. It handles several specialized cases:

1. **Regular Var nodes**: For user-defined columns (varattno > 0), it looks up the new attribute number in the provided mapping table and updates both varattno and varattnosyn fields. It validates that the attribute number exists in the mapping.

2. **Whole-row variables (varattno = 0)**: Sets a flag to notify the caller and optionally converts the variable to a different row type using ConvertRowtypeExpr if the context specifies a target row type.

3. **ConvertRowtypeExpr nodes**: Optimizes existing row type conversions on whole-row variables to avoid building stacks of conversion expressions by collapsing nested conversions.

4. **Query nodes**: Handles subqueries by managing sublevel tracking appropriately.

The function ensures that attribute number mappings are consistent and handles type coercion requirements for row type changes.

## Parameters / Member Variables
- : The current node being processed in the expression tree
- : Contains target RTE information, attribute mapping table, row type conversion settings, and sublevel tracking

## Dependencies
- Functions called/Symbols referenced:
  - map_variable_attnos_context (struct)
  - [ConvertRowtypeExpr](../C/ConvertRowtypeExpr.md) (node type)
  - COERCE_IMPLICIT_CAST (constant)
  - query_tree_mutator
  - expression_tree_mutator
  - [palloc](../p/palloc.md) (memory allocation)
  - makeNode (node creation)
- Called from (representative examples):
  - [map_variable_attnos](map_variable_attnos.md)
  - [map_variable_attnos_mutator](map_variable_attnos_mutator.md) (recursive calls)

## Notes and Other Information
- This is a static function, only accessible within the rewriteManip.c file
- Validates attribute mappings and raises errors for unexpected attribute numbers
- Handles both syntactic (varnosyn/varattnosyn) and semantic (varno/varattno) variable references
- Optimizes ConvertRowtypeExpr stacking to prevent performance degradation from repeated applications
- RECORD variables are explicitly not supported for row type conversion
- The function carefully preserves all other Var fields while only modifying attribute numbers and types as needed
- Whole-row variable detection is communicated back to the caller through the found_whole_row flag