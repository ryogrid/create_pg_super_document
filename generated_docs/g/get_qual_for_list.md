# get_qual_for_list

## Location
[src/backend/partitioning/partbounds.c:4066-4274](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L4066-L4274)

## Overview
Generates a CHECK constraint expression for a list partition by creating equality comparisons with allowed values and handling NULL values appropriately.

## Definition
static List *get_qual_for_list(Relation parent, PartitionBoundSpec *spec)

## Detailed Description
This function constructs the partition constraint for a list partition, which validates that the partition key equals one of the allowed values specified in the partition definition. The function handles both regular list partitions (with explicit value lists) and default list partitions (which accept values not in any other partition).

For regular list partitions, it creates equality expressions comparing the partition key to each allowed value. For default partitions, it generates a constraint that excludes all values used by other partitions. The function also properly handles NULL values, creating appropriate IS NULL or IS NOT NULL tests based on whether the partition accepts nulls.

The constraint generation includes special logic for single-column list partitioning and ensures proper null handling based on the partition specification.

## Parameters / Member Variables
- parent: The parent relation that is being partitioned
- spec: Partition bound specification containing the list of allowed values and default partition flag

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetPartitionKey](../R/RelationGetPartitionKey.md)
  - [makeVar](../m/makeVar.md)
  - copyObject
  - [RelationGetPartitionDesc](../R/RelationGetPartitionDesc.md)
  - partition_bound_accepts_nulls
  - [makeConst](../m/makeConst.md)
  - [datumCopy](../d/datumCopy.md)
  - [make_partition_op_expr](../m/make_partition_op_expr.md)
  - makeNode
  - [makeBoolExpr](../m/makeBoolExpr.md)
  - [make_ands_explicit](../m/make_ands_explicit.md)
- Called from (representative examples):
  - [get_qual_from_partbound](get_qual_from_partbound.md)
  - [check_default_partition_contents](../c/check_default_partition_contents.md)

## Notes and Other Information
- Only supports single-column list partitioning (key->partnatts == 1)
- For default partitions that are the only partition, returns NIL (no constraint needed)
- Handles both attribute-based and expression-based partition keys
- Creates IS NOT NULL tests for partitions that dont accept nulls
- Creates IS NULL tests for partitions that do accept nulls
- For default partitions, applies NOT to the entire constraint expression to exclude other partition values
- The generated constraints never evaluate to NULL, making NOT application work as intended

## Simplified Source

```c
static List *get_qual_for_list(Relation parent, PartitionBoundSpec *spec) {
    PartitionKey key = RelationGetPartitionKey(parent);
    List *elems = NIL;
    bool list_has_null = false;

    // Get partition key column expression
    if (key->partattrs[0] != 0)
        keyCol = (Expr *) makeVar(1, key->partattrs[0], key->parttypid[0],
                                 key->parttypmod[0], key->parttypcoll[0], 0);
    else
        keyCol = (Expr *) copyObject(linitial(key->partexprs));

    if (spec->is_default) {
        // For default partition, collect all other partition values
        PartitionDesc pdesc = RelationGetPartitionDesc(parent, false);
        if (pdesc->boundinfo) {
            for (int i = 0; i < pdesc->boundinfo->ndatums; i++) {
                Const *val = makeConst(/* create constant from bound datum */);
                elems = lappend(elems, val);
            }
        }
        if (partition_bound_accepts_nulls(pdesc->boundinfo))
            list_has_null = true;
    } else {
        // For regular partition, use specified list values
        foreach(cell, spec->listdatums) {
            Const *val = lfirst_node(Const, cell);
            if (val->constisnull)
                list_has_null = true;
            else
                elems = lappend(elems, copyObject(val));
        }
    }

    // Create equality expression for non-null values
    if (elems) {
        opexpr = make_partition_op_expr(key, 0, BTEqualStrategyNumber,
                                       keyCol, (Expr *) elems);
    }

    // Handle NULL value constraints
    if (!list_has_null) {
        // Add IS NOT NULL test
        nulltest = makeNode(NullTest);
        nulltest->arg = keyCol;
        nulltest->nulltesttype = IS_NOT_NULL;
        result = opexpr ? list_make2(nulltest, opexpr) : list_make1(nulltest);
    } else {
        // Add IS NULL test and combine with OR
        nulltest = makeNode(NullTest);
        nulltest->arg = keyCol;
        nulltest->nulltesttype = IS_NULL;
        if (opexpr) {
            result = list_make1(makeBoolExpr(OR_EXPR, list_make2(nulltest, opexpr), -1));
        } else {
            result = list_make1(nulltest);
        }
    }

    // For default partitions, negate the entire constraint
    if (spec->is_default) {
        result = list_make1(make_ands_explicit(result));
        result = list_make1(makeBoolExpr(NOT_EXPR, result, -1));
    }

    return result;
}
```