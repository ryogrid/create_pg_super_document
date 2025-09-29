# pull_varattnos_walker

## Location
[src/backend/optimizer/util/var.c:304-334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/var.c#L304-L334)

## Overview
The core walker function that traverses expression trees to collect attribute numbers from Var nodes matching a specific varno at level zero.

## Definition
```c
static bool pull_varattnos_walker(Node *node, pull_varattnos_context *context)
```

## Detailed Description
The `pull_varattnos_walker` function is a specialized tree walker that focuses specifically on extracting attribute numbers from Var nodes. Unlike the more complex `pull_varnos_walker`, this function has a simpler implementation focused solely on attribute collection.

The function only processes Var nodes that match both the target varno and are at varlevelsup 0 (current query level). When such a Var is found, it adds the variable's attribute number to the context's varattnos bitmapset, applying the FirstLowInvalidHeapAttributeNumber offset to accommodate system attributes.

The function explicitly asserts that it should not encounter unplanned Query nodes, indicating its intended use on expressions that have already undergone initial query planning phases.

## Parameters / Member Variables
- `node`: The current Node being examined in the tree traversal
- `context`: Walker context containing:
  - `varattnos`: Accumulating bitmapset of discovered attribute numbers
  - `varno`: Target relation number to match against Var nodes

## Dependencies
- Functions called/Symbols referenced:
  - [pull_varattnos_context](pull_varattnos_context.md) (walker context structure)
  - [bms_add_member](../b/bms_add_member.md) (bitmapset operation)
  - FirstLowInvalidHeapAttributeNumber (attribute offset constant)
  - expression_tree_walker (tree traversal)
  - [pull_varattnos_walker](pull_varattnos_walker.md) (recursive calls)
- Called from (representative examples):
  - [pull_varattnos](pull_varattnos.md)
  - [pull_varattnos_walker](pull_varattnos_walker.md) (recursive calls)

## Notes and Other Information
- Much simpler than pull_varnos_walker due to its focused scope on attribute collection
- Only handles level-zero variables (no nested query support)
- Uses attribute number offset to support system attributes in bitmap representation
- Includes assertion to catch unexpected unplanned subqueries
- Returns false to continue tree traversal in all cases
- Essential component for column-level analysis in PostgreSQL's optimizer

## Simplified Source

```c
static bool pull_varattnos_walker(Node *node, pull_varattnos_context *context) {
    if (node == NULL)
        return false;

    // Check if this is a Var node
    if (IsA(node, Var)) {
        Var *var = (Var *) node;

        // Only collect attributes from the target relation at current level
        if (var->varno == context->varno && var->varlevelsup == 0) {
            // Add attribute number to the bitmap (with offset for system attributes)
            context->varattnos = bms_add_member(context->varattnos,
                                               var->varattno - FirstLowInvalidHeapAttributeNumber);
        }
        return false;
    }

    // Ensure we don't encounter unplanned subqueries
    Assert(!IsA(node, Query));

    // Continue walking the expression tree
    return expression_tree_walker(node, pull_varattnos_walker, (void *) context);
}
```