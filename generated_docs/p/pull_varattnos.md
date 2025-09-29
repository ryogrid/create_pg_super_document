# pull_varattnos

## Location
[src/backend/optimizer/util/var.c:291-303](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/var.c#L291-L303)

## Overview
Extracts all distinct attribute numbers from an expression tree for variables of a specific varno, accumulating them in a provided bitmapset.

## Definition
```c
void pull_varattnos(Node *node, Index varno, Bitmapset **varattnos)
```

## Detailed Description
The `pull_varattnos` function finds all distinct attribute numbers present in an expression tree and adds them to the existing contents of the provided varattnos bitmapset. It specifically looks for Vars that match the given varno and are at rtable level zero.

The function uses an offset system where attribute numbers are adjusted by FirstLowInvalidHeapAttributeNumber, allowing the inclusion of system attributes (like OID) in the bitmap representation. Unlike the varno extraction functions, this function focuses on column-level analysis rather than relation-level analysis.

The function has limited subquery support - it handles already-planned SubPlan nodes by examining their "testexpr" and "args" lists, but does not support unplanned subqueries.

## Parameters / Member Variables
- `node`: The expression tree to analyze for attribute references
- `varno`: The specific relation (range table entry) number to examine
- `varattnos`: Pointer to bitmapset that accumulates the discovered attribute numbers (modified in-place)

## Dependencies
- Functions called/Symbols referenced:
  - [pull_varattnos_context](pull_varattnos_context.md) (struct used for walker context)
  - [pull_varattnos_walker](pull_varattnos_walker.md)
- Called from (representative examples):
  - [has_partition_attrs](../h/has_partition_attrs.md)
  - [DefineIndex](../D/DefineIndex.md)
  - [CreateStatistics](../C/CreateStatistics.md)
  - [ComputePartitionAttrs](../C/ComputePartitionAttrs.md)
  - [remove_unused_subquery_outputs](../r/remove_unused_subquery_outputs.md)
  - [check_index_only](../c/check_index_only.md)
  - [RelationGetIndexAttrBitmap](../R/RelationGetIndexAttrBitmap.md)

## Notes and Other Information
- Modifies the varattnos parameter in-place rather than returning a new bitmapset
- Attribute numbers are offset to accommodate system attributes in bitmap representation
- Only considers level-zero rtable entries (no support for nested query levels)
- Limited subquery support compared to the pull_varnos family of functions
- Commonly used for index analysis, statistics creation, and partition attribute handling
- Essential for column-level dependency analysis in query planning and DDL operations

## Simplified Source

```c
void
pull_varattnos(Node *node, Index varno, Bitmapset **varattnos)
{
    pull_varattnos_context context;

    context.varattnos = *varattnos;
    context.varno = varno;

    (void) pull_varattnos_walker(node, &context);

    *varattnos = context.varattnos;
}
```