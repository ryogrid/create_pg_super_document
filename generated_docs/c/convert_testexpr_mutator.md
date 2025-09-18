# convert_testexpr_mutator

## Location
[src/backend/optimizer/plan/subselect.c:654-711](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L654-L711)

## Overview
A tree-walking mutator function that recursively processes expression nodes to replace PARAM_SUBLINK parameters with their corresponding substitute nodes.

## Definition
```c
static Node *convert_testexpr_mutator(Node *node, convert_testexpr_context *context)
```

## Detailed Description
The `convert_testexpr_mutator` function implements a recursive tree walker that processes expression nodes to find and replace PARAM_SUBLINK parameters. This is the core implementation function called by `convert_testexpr` that performs the actual parameter substitution work.

The function handles three main cases:
1. **PARAM_SUBLINK Parameters**: When encountering a Param node with paramkind == PARAM_SUBLINK, it validates the parameter ID and replaces it with the corresponding node from the substitution list.
2. **Nested SubLinks**: When encountering a SubLink node, it returns it as-is without recursion, since PARAM_SUBLINKs within nested SubLinks belong to the inner SubLink and should not be processed by the outer conversion.
3. **Other Nodes**: For all other node types, it delegates to `expression_tree_mutator` to continue the recursive traversal.

The function includes careful error checking and uses `copyObject` to avoid creating doubly-linked substructure in the modified parse tree.

## Parameters / Member Variables
- `node`: The current expression node being processed during tree traversal
- `context`: Conversion context containing the PlannerInfo root and the list of substitute nodes

## Dependencies
- Functions called/Symbols referenced:
  - `copyObject`
  - [list_nth](../l/list_nth.md) 
  - `expression_tree_mutator`
  - [convert_testexpr_mutator](convert_testexpr_mutator.md) (recursive call)
- Types referenced:
  - `Param`
  - `SubLink`
  - `PARAM_SUBLINK`
  - [convert_testexpr_context](convert_testexpr_context.md)
- Called from (representative examples):
  - [convert_testexpr](convert_testexpr.md) (src/backend/optimizer/plan/subselect.c:650)
  - Self (recursive calls during tree traversal)

## Notes and Other Information
This function is critical for PostgreSQL's subquery processing pipeline. The special handling of nested SubLinks is important because PARAM_SUBLINKs are only unique per SubLink, not globally across the query. The conversion to globally unique parameters (Vars or PARAM_EXEC nodes) must happen before parameters escape from their originating SubLink's testexpr. The function can be called from different contexts: during SS_process_sublinks (where inner SubLinks are processed first) or from convert_ANY_sublink_to_join (where nested SubLinks might be encountered).