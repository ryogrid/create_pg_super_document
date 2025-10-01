# initialize_mergeclause_eclasses

## Location
[src/backend/optimizer/path/pathkeys.c:1443-1489](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L1443-L1489)

## Overview
Sets up EquivalenceClass links in a mergeclause RestrictInfo by finding or creating appropriate EquivalenceClasses for the left and right operands of the merge operation.

## Definition

```c
void
initialize_mergeclause_eclasses(PlannerInfo *root, RestrictInfo *restrictinfo)
```
## Detailed Description
The `initialize_mergeclause_eclasses` function establishes the equivalence class relationships for a mergeclause by setting the `left_ec` and `right_ec` fields in the RestrictInfo structure. This function is called when a mergeclause is not directly generated from or used to create an EquivalenceClass, requiring explicit setup of these relationships.

The function extracts the left and right operands from the merge operator expression and determines their declared input types. It then calls `get_eclass_for_sort_expr` to find existing EquivalenceClasses or create new ones for each operand. This setup is crucial for the query planner to understand which expressions can be considered equivalent for optimization purposes.

Note that this function is called before EquivalenceClass merging is complete, so the established links may not initially point to canonical ECs. The `update_mergeclause_eclasses` function must be called later to ensure the links point to the final canonical equivalence classes.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state and context
- `restrictinfo`: RestrictInfo structure for the mergeclause that needs EquivalenceClass links established

## Dependencies
- Functions called/Symbols referenced:
  - [op_input_types](../o/op_input_types.md)
  - [OpExpr](../O/OpExpr.md) (struct type)
  - [get_eclass_for_sort_expr](../g/get_eclass_for_sort_expr.md)
  - [get_leftop](../g/get_leftop.md)
  - [get_rightop](../g/get_rightop.md)
- Called from (representative examples):
  - [distribute_qual_to_rels](../d/distribute_qual_to_rels.md)

## Notes and Other Information
- Must be called on mergeclauses before EC merging is complete
- Requires that `restrictinfo->mergeopfamilies` is not NIL (validates it's actually a mergeclause)
- Asserts that left_ec and right_ec links are initially NULL (not yet set)
- Creates new EquivalenceClasses if necessary to represent the mergeclause operands
- The EquivalenceClasses created may be the same (for true equivalence clauses) or different
- Links established here are not necessarily canonical and require later updating via `update_mergeclause_eclasses`
- Essential for enabling merge join optimizations and understanding expression equivalences in query planning

## Simplified Source

```c
void initialize_mergeclause_eclasses(PlannerInfo *root, RestrictInfo *restrictinfo) {
    Expr *clause = restrictinfo->clause;
    Oid lefttype, righttype;

    // Validate this is a mergeclause with unset EC links
    Assert(restrictinfo->mergeopfamilies != NIL);
    Assert(restrictinfo->left_ec == NULL);
    Assert(restrictinfo->right_ec == NULL);

    // Get the input types for the merge operator
    op_input_types(((OpExpr *) clause)->opno, &lefttype, &righttype);

    // Find or create EquivalenceClass for left operand
    restrictinfo->left_ec = get_eclass_for_sort_expr(root,
                                                    (Expr *) get_leftop(clause),
                                                    restrictinfo->mergeopfamilies,
                                                    lefttype,
                                                    ((OpExpr *) clause)->inputcollid,
                                                    0, NULL, true);

    // Find or create EquivalenceClass for right operand
    restrictinfo->right_ec = get_eclass_for_sort_expr(root,
                                                     (Expr *) get_rightop(clause),
                                                     restrictinfo->mergeopfamilies,
                                                     righttype,
                                                     ((OpExpr *) clause)->inputcollid,
                                                     0, NULL, true);
}
```