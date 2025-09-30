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

## Simplified Source

```c
static Node *
map_variable_attnos_mutator(Node *node, map_variable_attnos_context *context)
{
    if (node == NULL)
        return NULL;

    if (IsA(node, Var))
    {
        Var *var = (Var *) node;

        // Check if this variable matches our target
        if (var->varno == context->target_varno &&
            var->varlevelsup == context->sublevels_up)
        {
            // Create new variable with updated attributes
            Var *newvar = (Var *) palloc(sizeof(Var));
            *newvar = *var;  // Copy all fields

            if (var->varattno > 0)
            {
                // Regular column: remap attribute number
                if (var->varattno > context->attno_map->maplen ||
                    context->attno_map->attnums[var->varattno - 1] == 0)
                    elog(ERROR, "unexpected varattno %d", var->varattno);

                newvar->varattno = context->attno_map->attnums[var->varattno - 1];

                // Update syntactic reference if needed
                if (newvar->varnosyn == context->target_varno)
                    newvar->varattnosyn = newvar->varattno;
            }
            else if (var->varattno == 0)
            {
                // Whole-row variable: notify caller and handle type conversion
                *(context->found_whole_row) = true;

                if (OidIsValid(context->to_rowtype) &&
                    context->to_rowtype != var->vartype)
                {
                    // Convert to new row type
                    ConvertRowtypeExpr *conversion = makeNode(ConvertRowtypeExpr);
                    newvar->vartype = context->to_rowtype;

                    conversion->arg = (Expr *) newvar;
                    conversion->resulttype = var->vartype;
                    conversion->convertformat = COERCE_IMPLICIT_CAST;
                    conversion->location = -1;

                    return (Node *) conversion;
                }
            }
            return (Node *) newvar;
        }
    }
    else if (IsA(node, ConvertRowtypeExpr))
    {
        ConvertRowtypeExpr *conversion = (ConvertRowtypeExpr *) node;
        Var *var = (Var *) conversion->arg;

        // Optimize nested conversions for whole-row variables
        if (IsA(var, Var) &&
            var->varno == context->target_varno &&
            var->varlevelsup == context->sublevels_up &&
            var->varattno == 0 &&
            OidIsValid(context->to_rowtype) &&
            context->to_rowtype != var->vartype)
        {
            // Create optimized conversion without stacking
            ConvertRowtypeExpr *newnode = (ConvertRowtypeExpr *) palloc(sizeof(ConvertRowtypeExpr));
            Var *newvar = (Var *) palloc(sizeof(Var));

            *(context->found_whole_row) = true;
            *newvar = *var;
            newvar->vartype = context->to_rowtype;

            *newnode = *conversion;
            newnode->arg = (Expr *) newvar;

            return (Node *) newnode;
        }
    }
    else if (IsA(node, Query))
    {
        // Handle subqueries with proper sublevel tracking
        context->sublevels_up++;
        Query *newnode = query_tree_mutator((Query *) node,
                                          map_variable_attnos_mutator,
                                          context, 0);
        context->sublevels_up--;
        return (Node *) newnode;
    }

    // Recursively process other node types
    return expression_tree_mutator(node, map_variable_attnos_mutator, context);
}
```