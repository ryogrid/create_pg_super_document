# DetachAddConstraintIfNeeded

## Location
[src/backend/commands/tablecmds.c:19681-19721](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L19681-L19721)

## Overview
DetachAddConstraintIfNeeded creates a check constraint equivalent to the partition constraint on a partition being detached, but only if no existing constraint already implies the needed constraint.

## Definition
```c
static void DetachAddConstraintIfNeeded(List **wqueue, Relation partRel)
```

## Detailed Description
This function is a subroutine of ATExecDetachPartition that handles the critical step of ensuring a partition maintains its constraint validation after being detached from its parent table. When a partition is detached, it loses the implicit partition constraint that was enforced by its membership in the partitioned table hierarchy.

The function performs the following operations:

1. **Constraint extraction**: Retrieves the partition constraint expression from the partition's metadata
2. **Expression optimization**: Applies constant expression evaluation to simplify the constraint
3. **Redundancy check**: Uses PartConstraintImpliedByRelConstraint to determine if an existing constraint already covers the needed validation
4. **Constraint creation**: If needed, creates a new check constraint that replicates the partition constraint logic

This ensures that after detachment, the former partition continues to enforce the same data validation rules that were previously guaranteed by its partition membership.

## Parameters / Member Variables
- `wqueue`: Pointer to the ALTER TABLE work queue list for managing related constraint operations
- `partRel`: The partition relation that needs a constraint added

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetPartitionQual](../R/RelationGetPartitionQual.md)
  - [eval_const_expressions](../e/eval_const_expressions.md)
  - [PartConstraintImpliedByRelConstraint](../P/PartConstraintImpliedByRelConstraint.md)
  - [ATGetQueueEntry](../A/ATGetQueueEntry.md)
  - makeNode
  - [make_ands_explicit](../m/make_ands_explicit.md)
  - [nodeToString](../n/nodeToString.md)
  - [ATAddCheckConstraint](../A/ATAddCheckConstraint.md)
- Called from (representative examples):
  - [ATExecDetachPartition](../A/ATExecDetachPartition.md)

## Notes and Other Information
- Only creates constraints when necessary - avoids duplicate constraints through PartConstraintImpliedByRelConstraint check
- Uses ShareUpdateExclusiveLock when adding the constraint to allow concurrent reads
- The constraint is marked as initially_valid and skip_validation since the data is already known to satisfy it
- Sets is_no_inherit to false, allowing inheritance if the detached table later becomes a parent
- Essential for maintaining data integrity in concurrent detachment scenarios
- The created constraint exactly replicates the logical validation that was previously implicit

## Simplified Source
```c
static void DetachAddConstraintIfNeeded(List **wqueue, Relation partRel) {
    List *constraintExpr;

    // Get the partition constraint expression and optimize it
    constraintExpr = RelationGetPartitionQual(partRel);
    constraintExpr = (List *) eval_const_expressions(NULL, (Node *) constraintExpr);

    // Check if existing constraints already imply the needed constraint
    if (!PartConstraintImpliedByRelConstraint(partRel, constraintExpr)) {
        AlteredTableInfo *tab;
        Constraint *new_constraint;

        // Get work queue entry for this table
        tab = ATGetQueueEntry(wqueue, partRel);

        // Create new check constraint equivalent to partition constraint
        new_constraint = makeNode(Constraint);
        new_constraint->contype = CONSTR_CHECK;
        new_constraint->conname = NULL; // Auto-generate name
        new_constraint->location = -1;
        new_constraint->is_no_inherit = false;
        new_constraint->raw_expr = NULL;
        new_constraint->cooked_expr = nodeToString(make_ands_explicit(constraintExpr));
        new_constraint->initially_valid = true;
        new_constraint->skip_validation = true; // Data already satisfies constraint

        // Add the constraint to the work queue
        ATAddCheckConstraint(wqueue, tab, partRel, new_constraint,
                           true, false, true, ShareUpdateExclusiveLock);
    }
}
```