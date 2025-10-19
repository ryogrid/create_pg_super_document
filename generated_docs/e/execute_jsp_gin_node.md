# execute_jsp_gin_node

## Location
[src/backend/utils/adt/jsonb_gin.c:799-847](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_gin.c#L799-L847)

## Overview
Recursively evaluates a JsonPathGinNode expression tree using GIN index entry match results, implementing three-valued logic (TRUE/FALSE/MAYBE) for jsonpath query consistency checking.

## Definition

```c
static GinTernaryValue
execute_jsp_gin_node(JsonPathGinNode *node, void *check, bool ternary)
```
## Detailed Description
This function serves as the execution engine for jsonpath GIN queries, evaluating the logical expression tree built during query extraction. It implements three-valued logic where GIN_TRUE indicates a definite match, GIN_FALSE indicates a definite non-match, and GIN_MAYBE indicates uncertainty requiring further verification.

The function handles three types of nodes:
- JSP_GIN_AND: Implements logical AND with short-circuiting (returns FALSE immediately if any child is FALSE)
- JSP_GIN_OR: Implements logical OR with short-circuiting (returns TRUE immediately if any child is TRUE)  
- JSP_GIN_ENTRY: Retrieves the match result for a specific GIN entry from the check array

The ternary flag determines whether the check array contains boolean values or GinTernaryValue enums, allowing the function to work with both simple boolean matching and more sophisticated tri-state logic used in advanced GIN consistency checking.

## Parameters / Member Variables
- `*node`: JsonPathGinNode pointer to the current expression node being evaluated
- `*check`: Void pointer to an array containing match results - either bool[] or GinTernaryValue[] depending on ternary flag
- `ternary`: Boolean flag indicating whether check array contains GinTernaryValue (true) or bool (false) elements
## Dependencies
- Functions called/Symbols referenced:
  - [execute_jsp_gin_node](execute_jsp_gin_node.md) (recursive self-calls for child node evaluation)
  - elog (error logging for invalid node types)
- Called from (representative examples):
  - [gin_consistent_jsonb](../g/gin_consistent_jsonb.md) (boolean consistency checking for jsonb_ops)
  - [gin_triconsistent_jsonb](../g/gin_triconsistent_jsonb.md) (ternary consistency checking for jsonb_ops)
  - [gin_consistent_jsonb_path](../g/gin_consistent_jsonb_path.md) (boolean consistency checking for jsonb_path_ops)
  - [gin_triconsistent_jsonb_path](../g/gin_triconsistent_jsonb_path.md) (ternary consistency checking for jsonb_path_ops)
  - [execute_jsp_gin_node](execute_jsp_gin_node.md) (recursive self-calls for nested expressions)

## Notes and Other Information
- Implements proper three-valued logic semantics with short-circuiting optimization
- The function assumes that entry indices in JSP_GIN_ENTRY nodes are valid array indices
- Short-circuiting behavior improves performance by avoiding unnecessary evaluations
- The ternary parameter allows the same function to work with both boolean and tri-state logic
- Error handling for invalid node types suggests the function expects only specific node types in valid query trees

## Simplified Source

```c
static GinTernaryValue
execute_jsp_gin_node(JsonPathGinNode *node, void *check, bool ternary)
{
    GinTernaryValue res, v;
    int i;

    switch (node->type)
    {
        case JSP_GIN_AND:
            // Logical AND with short-circuiting
            res = GIN_TRUE;
            for (i = 0; i < node->val.nargs; i++)
            {
                v = execute_jsp_gin_node(node->args[i], check, ternary);
                if (v == GIN_FALSE)
                    return GIN_FALSE;  // Short-circuit on first FALSE
                else if (v == GIN_MAYBE)
                    res = GIN_MAYBE;   // Remember if any child is uncertain
            }
            return res;

        case JSP_GIN_OR:
            // Logical OR with short-circuiting
            res = GIN_FALSE;
            for (i = 0; i < node->val.nargs; i++)
            {
                v = execute_jsp_gin_node(node->args[i], check, ternary);
                if (v == GIN_TRUE)
                    return GIN_TRUE;   // Short-circuit on first TRUE
                else if (v == GIN_MAYBE)
                    res = GIN_MAYBE;   // Remember if any child is uncertain
            }
            return res;

        case JSP_GIN_ENTRY:
            // Look up entry match result from check array
            int index = node->val.entryIndex;
            if (ternary)
                return ((GinTernaryValue *) check)[index];
            else
                return ((bool *) check)[index] ? GIN_TRUE : GIN_FALSE;

        default:
            elog(ERROR, "invalid jsonpath gin node type: %d", node->type);
            return GIN_FALSE;
    }
}
```