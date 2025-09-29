# check_default_partition_contents

## Location
[src/backend/partitioning/partbounds.c:3251-3413](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L3251-L3413)

## Overview
This function validates that adding a new partition will not violate constraints by checking if any rows in the default partition would properly belong to the new partition being added.

## Definition

```c
void
check_default_partition_contents(Relation parent, Relation default_rel,
								 PartitionBoundSpec *new_spec)
```
## Detailed Description
The  function performs a critical validation step during partition creation. When a new partition is being added to a partitioned table that has a default partition, this function ensures that no existing rows in the default partition would belong to the new partition's range or list values. If such rows are found, it throws an error to prevent constraint violations.

The function operates by:
1. Generating partition constraints for the new partition based on its specification
2. Creating corresponding default partition constraints that exclude the new partition's range
3. Optimizing by checking if existing constraints already guarantee no conflicts
4. Scanning the default partition (and its subpartitions if it's partitioned) to validate no violating rows exist
5. For each partition, executing the constraint check against every row

The function handles both LIST and RANGE partitioning strategies and properly handles nested partitioning scenarios where the default partition itself may be partitioned.

## Parameters / Member Variables
- : The parent partitioned table relation
- : The default partition relation to be checked  
- : The partition bound specification for the new partition being added

## Dependencies
- Functions called/Symbols referenced:
  - [get_qual_for_list](../g/get_qual_for_list.md) / get_qual_for_range
  - [get_proposed_default_constraint](../g/get_proposed_default_constraint.md)
  - [map_partition_varattnos](../m/map_partition_varattnos.md)
  - [PartConstraintImpliedByRelConstraint](../P/PartConstraintImpliedByRelConstraint.md)
  - [find_all_inheritors](../f/find_all_inheritors.md)
  - [make_ands_explicit](../m/make_ands_explicit.md)
  - [CreateExecutorState](../C/CreateExecutorState.md) / ExecPrepareExpr / ExecCheck
  - [table_beginscan](../t/table_beginscan.md) / table_scan_getnextslot / table_endscan
- Called from (representative examples):
  - [DefineRelation](../D/DefineRelation.md) (during partition creation)

## Notes and Other Information
- The function performs an optimization by checking if existing partition constraints already imply that no violating rows exist, avoiding expensive table scans when possible
- For foreign tables within default partitions, the function issues a warning and skips scanning since foreign data cannot be validated
- The function uses PostgreSQL's executor framework for efficient constraint evaluation during table scanning
- Memory management is carefully handled with per-tuple contexts to avoid memory leaks during large table scans
- All scanning is performed under AccessExclusiveLock to ensure data consistency during the validation process

## Simplified Source

```c
void check_default_partition_contents(Relation parent, Relation default_rel,
                                     PartitionBoundSpec *new_spec) {
    // Generate partition constraints for the new partition
    List *new_part_constraints;
    if (new_spec->strategy == PARTITION_STRATEGY_LIST)
        new_part_constraints = get_qual_for_list(parent, new_spec);
    else
        new_part_constraints = get_qual_for_range(parent, new_spec, false);

    // Create default partition constraints (negated new partition constraints)
    List *default_constraints = get_proposed_default_constraint(new_part_constraints);

    // Map constraints to default partition's attribute numbers
    default_constraints = map_partition_varattnos(default_constraints, 1,
                                                 default_rel, parent);

    // Optimization: check if existing constraints already guarantee no conflicts
    if (PartConstraintImpliedByRelConstraint(default_rel, default_constraints)) {
        // No scan needed - constraints prove no violating rows exist
        return;
    }

    // Scan default partition and all its child partitions
    List *all_parts;
    if (default_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
        all_parts = find_all_inheritors(RelationGetRelid(default_rel),
                                       AccessExclusiveLock, NULL);
    else
        all_parts = list_make1_oid(RelationGetRelid(default_rel));

    // Check each partition for constraint violations
    ListCell *lc;
    foreach(lc, all_parts) {
        Oid part_relid = lfirst_oid(lc);
        Relation part_rel = (part_relid != RelationGetRelid(default_rel)) ?
                            table_open(part_relid, NoLock) : default_rel;

        // Skip non-leaf partitions
        if (part_rel->rd_rel->relkind != RELKIND_RELATION) {
            if (part_rel != default_rel)
                table_close(part_rel, NoLock);
            continue;
        }

        // Set up constraint checking
        EState *estate = CreateExecutorState();
        Expr *constraint_expr = make_ands_explicit(default_constraints);
        ExprState *constraint_state = ExecPrepareExpr(constraint_expr, estate);

        // Scan table and check each row
        ExprContext *econtext = GetPerTupleExprContext(estate);
        Snapshot snapshot = RegisterSnapshot(GetLatestSnapshot());
        TupleTableSlot *slot = table_slot_create(part_rel, &estate->es_tupleTable);
        TableScanDesc scan = table_beginscan(part_rel, snapshot, 0, NULL);

        while (table_scan_getnextslot(scan, ForwardScanDirection, slot)) {
            econtext->ecxt_scantuple = slot;

            // If constraint is violated, error out
            if (!ExecCheck(constraint_state, econtext)) {
                ereport(ERROR, (errcode(ERRCODE_CHECK_VIOLATION),
                        errmsg("default partition contains row that would belong to new partition")));
            }

            ResetExprContext(econtext);
            CHECK_FOR_INTERRUPTS();
        }

        // Cleanup
        table_endscan(scan);
        UnregisterSnapshot(snapshot);
        ExecDropSingleTupleTableSlot(slot);
        FreeExecutorState(estate);

        if (part_rel != default_rel)
            table_close(part_rel, NoLock);
    }
}
```