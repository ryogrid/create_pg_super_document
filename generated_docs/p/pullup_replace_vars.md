# pullup_replace_vars

## Location
[src/backend/optimizer/prep/prepjointree.c:2474-2483](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L2474-L2483)

## Overview
Applies pullup variable replacement throughout an expression tree by delegating to the generic replace_rte_variables function with a specialized callback.

## Definition
```c
static Node *pullup_replace_vars(Node *expr, pullup_replace_vars_context *context)
```

## Detailed Description
This function serves as a simple wrapper around the generic `replace_rte_variables` function, providing pullup-specific behavior through the `pullup_replace_vars_callback`. It processes an expression tree and replaces all variable references to a pulled-up subquery with appropriate substitute expressions.

The function delegates the actual tree traversal and variable identification to `replace_rte_variables`, which calls back to `pullup_replace_vars_callback` for each variable found that matches the target RTE. This design allows the generic tree-walking logic to be reused while providing specialized replacement behavior for the pullup context.

The function returns a modified copy of the expression tree, making it suitable for use in contexts where the original tree must be preserved.

## Parameters / Member Variables
- `expr`: The expression tree node to process for variable replacement
- `context`: Context structure containing substitution mappings, target RTE information, and control flags

## Dependencies
- Functions called/Symbols referenced:
  - [replace_rte_variables](../r/replace_rte_variables.md)
  - [pullup_replace_vars_callback](pullup_replace_vars_callback.md)
  - [pullup_replace_vars_context](pullup_replace_vars_context.md)
- Called from (representative examples):
  - [perform_pullup_replace_vars](perform_pullup_replace_vars.md) (multiple locations)
  - [replace_vars_in_jointree](../r/replace_vars_in_jointree.md) (multiple locations)

## Notes and Other Information
- Returns a modified copy of the tree rather than performing in-place replacement
- Uses the generic replace_rte_variables infrastructure with pullup-specific callback logic
- The `outer_hasSubLinks` parameter from context is passed through to handle sublink processing appropriately
- Acts as a thin wrapper that bridges between the pullup-specific context and the generic variable replacement infrastructure

## Simplified Source

```c
static Node *pullup_replace_vars(Node *expr, pullup_replace_vars_context *context)
{
    return replace_rte_variables(expr,
                                context->varno, 0,
                                pullup_replace_vars_callback,
                                (void *) context,
                                context->outer_hasSubLinks);
}
```