# pull_var_clause_walker

## Location
[src/backend/optimizer/util/var.c:627-743](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/var.c#L627-L743)

## Overview
A static tree walker function that implements the core logic for extracting Var nodes and other specified node types from an expression tree, with configurable behavior for aggregates, window functions, and placeholders.

## Definition
```c
static bool pull_var_clause_walker(Node *node, pull_var_clause_context *context)
```

## Detailed Description
This function performs the actual tree traversal for `pull_var_clause()`, examining each node and taking appropriate actions based on the node type and the flags specified in the context. It implements sophisticated handling for different classes of nodes that may contain or represent variables.

**Node Type Handling:**

- **Var nodes**: Added directly to the output list. Validates that `varlevelsup == 0` (no upper-level variables expected).

- **Aggref nodes**: Behavior depends on flags:
  - `PVC_INCLUDE_AGGREGATES`: Add the Aggref to the list and stop traversing into it
  - `PVC_RECURSE_AGGREGATES`: Continue traversing into the aggregate's arguments 
  - Neither: Error if found
  - Validates that `agglevelsup == 0`

- **GroupingFunc nodes**: Treated identically to Aggref nodes, using the same PVC_*_AGGREGATES flags and level validation.

- **WindowFunc nodes**: Behavior depends on flags:
  - `PVC_INCLUDE_WINDOWFUNCS`: Add the WindowFunc to the list and stop traversing
  - `PVC_RECURSE_WINDOWFUNCS`: Continue traversing into the function's arguments
  - Neither: Error if found
  - Note: WindowFuncs have no levelsup field to validate

- **PlaceHolderVar nodes**: Behavior depends on flags:
  - `PVC_INCLUDE_PLACEHOLDERS`: Add the PlaceHolderVar to the list and stop traversing
  - `PVC_RECURSE_PLACEHOLDERS`: Continue traversing into the placeholder's expression
  - Neither: Error if found
  - Validates that `phlevelsup == 0`

The function maintains the invariant that upper-level references (levelsup > 0) should not be present, throwing errors when violations are detected.

## Parameters / Member Variables
- `node`: The current node being examined in the expression tree traversal
- `context`: Pointer to the context structure containing:
  - `varlist`: The output list where matching nodes are accumulated
  - `flags`: Bitmask controlling behavior for different node types

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - elog (for error reporting)
  - [lappend](../l/lappend.md) (for adding nodes to the output list)
  - expression_tree_walker (for recursing into child nodes)
- Constants used:
  - PVC_INCLUDE_AGGREGATES, PVC_RECURSE_AGGREGATES
  - PVC_INCLUDE_WINDOWFUNCS, PVC_RECURSE_WINDOWFUNCS
  - PVC_INCLUDE_PLACEHOLDERS, PVC_RECURSE_PLACEHOLDERS
- [Node](../N/Node.md) types handled:
  - [Var](../V/Var.md), Aggref, GroupingFunc, WindowFunc, PlaceHolderVar
- Data structures used:
  - pull_var_clause_context
- Called from:
  - [pull_var_clause](pull_var_clause.md) (main entry point)
  - [pull_var_clause_walker](pull_var_clause_walker.md) (recursive self-calls via expression_tree_walker)

## Notes and Other Information
- This is a static helper function, not part of the public API
- The function never returns true (always continues or delegates to expression_tree_walker)
- Enforces strict validation that no upper-level references should be present
- Uses lappend to build the result list, maintaining order of discovery
- [GroupingFunc](../G/GroupingFunc.md) nodes are treated as a special case of aggregate functions
- [WindowFunc](../W/WindowFunc.md) handling is unique in not having level validation since WindowFuncs lack a levelsup field
- When INCLUDE flags are used, the function stops recursing into that node type
- When RECURSE flags are used, traversal continues to find Vars within the node's arguments
- The "fall through" pattern allows RECURSE flags to continue to the general expression_tree_walker call

## Simplified Source

```c
static bool
pull_var_clause_walker(Node *node, pull_var_clause_context *context)
{
    if (node == NULL)
        return false;

    if (IsA(node, Var))
    {
        // Check for upper-level variables
        if (((Var *) node)->varlevelsup != 0)
            elog(ERROR, "Upper-level Var found where not expected");

        // Add Var to result list
        context->varlist = lappend(context->varlist, node);
        return false;
    }
    else if (IsA(node, Aggref))
    {
        // Validate level and handle based on flags
        if (((Aggref *) node)->agglevelsup != 0)
            elog(ERROR, "Upper-level Aggref found where not expected");

        if (context->flags & PVC_INCLUDE_AGGREGATES)
        {
            context->varlist = lappend(context->varlist, node);
            return false;  // Don't recurse into aggregate
        }
        else if (context->flags & PVC_RECURSE_AGGREGATES)
        {
            // Fall through to recurse into arguments
        }
        else
            elog(ERROR, "Aggref found where not expected");
    }
    else if (IsA(node, GroupingFunc))
    {
        // Handle GROUPING() functions like aggregates
        if (((GroupingFunc *) node)->agglevelsup != 0)
            elog(ERROR, "Upper-level GROUPING found where not expected");

        if (context->flags & PVC_INCLUDE_AGGREGATES)
        {
            context->varlist = lappend(context->varlist, node);
            return false;
        }
        else if (context->flags & PVC_RECURSE_AGGREGATES)
        {
            // Fall through to recurse
        }
        else
            elog(ERROR, "GROUPING found where not expected");
    }
    else if (IsA(node, WindowFunc))
    {
        // Handle window functions
        if (context->flags & PVC_INCLUDE_WINDOWFUNCS)
        {
            context->varlist = lappend(context->varlist, node);
            return false;
        }
        else if (context->flags & PVC_RECURSE_WINDOWFUNCS)
        {
            // Fall through to recurse
        }
        else
            elog(ERROR, "WindowFunc found where not expected");
    }
    else if (IsA(node, PlaceHolderVar))
    {
        // Handle placeholder variables
        if (((PlaceHolderVar *) node)->phlevelsup != 0)
            elog(ERROR, "Upper-level PlaceHolderVar found where not expected");

        if (context->flags & PVC_INCLUDE_PLACEHOLDERS)
        {
            context->varlist = lappend(context->varlist, node);
            return false;
        }
        else if (context->flags & PVC_RECURSE_PLACEHOLDERS)
        {
            // Fall through to recurse
        }
        else
            elog(ERROR, "PlaceHolderVar found where not expected");
    }

    // Continue walking the expression tree
    return expression_tree_walker(node, pull_var_clause_walker,
                                  (void *) context);
}
```