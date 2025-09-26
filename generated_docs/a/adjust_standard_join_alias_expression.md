# adjust_standard_join_alias_expression

## Location
[src/backend/optimizer/util/var.c:1036-1097](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/var.c#L1036-L1097)

## Overview
Modifies a standard join alias expression in-place to integrate nullingrels information from an original Var.

## Definition
```c
static void adjust_standard_join_alias_expression(Node *newnode, Var *oldvar)
```

## Detailed Description
This function performs the actual nullingrels integration for expressions that have been validated by is_standard_join_alias_expression. It implements a recursive tree-walking algorithm that mirrors the structure validation logic but performs modifications instead of checks.

**Modification Strategies by Node Type**:

1. **Var Nodes**: Directly merges oldvar's varnullingrels with the Var's existing varnullingrels using bms_add_members. Only processes Vars at the matching query level.

2. **PlaceHolderVar Nodes**: Merges nullingrels into the phnullingrels field. Again, only processes PlaceHolderVars at the matching query level.

3. **Coercion Expressions** (FuncExpr, RelabelType, CoerceViaIO, ArrayCoerceExpr): These are transparent with respect to nullingrels, so the function recursively processes their arguments. For FuncExpr, only the first argument is processed (additional arguments are typically constants).

4. **CoalesceExpr**: Recursively processes all arguments since COALESCE semantics require that nullingrels apply to all operands.

**Key Behavioral Characteristics**:
- Performs in-place modification of the expression tree
- Uses bms_add_members to merge rather than replace nullingrels (preserving existing nullingrels)
- Maintains the same recursive structure as is_standard_join_alias_expression
- Asserts on unrecognized node types (should never occur if preceded by proper validation)

The function is designed to be called only after is_standard_join_alias_expression returns true, ensuring that all encountered nodes are of expected types.

## Parameters / Member Variables
- `newnode`: The standard join alias expression to be modified (modified in-place)
- `oldvar`: The original Var containing varnullingrels to be integrated

## Dependencies
- Functions called/Symbols referenced:
  - [bms_add_members](../b/bms_add_members.md) (for merging nullingrels bitmaps)
  - [PlaceHolderVar](../P/PlaceHolderVar.md), FuncExpr, RelabelType
  - [CoerceViaIO](../C/CoerceViaIO.md), ArrayCoerceExpr, CoalesceExpr
  - linitial (for accessing first arguments)
  - Recursive calls to adjust_standard_join_alias_expression
- Called from (representative examples):
  - [add_nullingrels_if_needed](add_nullingrels_if_needed.md) (when direct integration is possible)

## Notes and Other Information
- The function assumes proper prior validation by is_standard_join_alias_expression
- Uses bms_add_members rather than assignment to preserve existing nullingrels information
- [Query](../Q/Query.md) level matching ensures that only appropriate Vars and PlaceHolderVars are modified
- The Assert(false) at the end serves as a safety net for unexpected node types
- Coercion expressions are handled transparently since they don't affect null semantics
- COALESCE processing applies nullingrels to all arguments since any of them might contribute to the final result