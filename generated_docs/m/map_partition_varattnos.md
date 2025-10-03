# map_partition_varattnos

## Location
[src/backend/catalog/partition.c:222-254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/partition.c#L222-L254)

## Overview
Maps variable attribute numbers (varattnos) in expressions from one relation to another within the same partitioning hierarchy, handling cases where column positions differ between partitioned tables and their partitions.

## Definition

```c
List *
map_partition_varattnos(List *expr, int fromrel_varno,
						Relation to_rel, Relation from_rel)
```
## Detailed Description
This function transforms expressions by mapping variable attribute numbers from one relation to another in a partitioning hierarchy. Even though partitioned tables and their partitions must have the same column names and types, their physical attribute numbers (attnums) may differ. The function uses name-based attribute mapping to convert expressions so they reference the correct columns in the target relation.

The function works by:
1. Building an attribute map using  to match columns by name between relations
2. Using  to transform all variable references in the expression
3. Handling whole-row variable references appropriately

## Parameters / Member Variables
- `*expr`: List of expression nodes containing variables to be remapped
- `fromrel_varno`: The range table entry number of the source relation in the expression
- `to_rel`: Target relation to map attribute numbers to
- `from_rel`: Source relation to map attribute numbers from
## Dependencies
- Functions called/Symbols referenced:
  - [build_attrmap_by_name](../b/build_attrmap_by_name.md)
  - [map_variable_attnos](map_variable_attnos.md)
  - RelationGetDescr (via macro)
  - RelationGetForm (via macro)
- Called from (representative examples):
  - [QueuePartitionConstraintValidation](../Q/QueuePartitionConstraintValidation.md)
  - [ATExecAttachPartition](../A/ATExecAttachPartition.md)
  - [CloneRowTriggersToPartition](../C/CloneRowTriggersToPartition.md)
  - [CreateTriggerFiringOn](../C/CreateTriggerFiringOn.md)
  - [check_default_partition_contents](../c/check_default_partition_contents.md)
  - [generate_partition_qual](../g/generate_partition_qual.md)

## Notes and Other Information
- The function can work on any node tree structure, not just Lists, but uses List type for convenience since most callers work with Lists
- Both relations must be from the same partitioning hierarchy
- The function handles NIL input gracefully by returning it unchanged
- Whole-row variable handling is managed internally and the found_whole_row result is ignored since a target row type is provided

## Simplified Source

```c
List *map_partition_varattnos(List *expr, int fromrel_varno,
                             Relation to_rel, Relation from_rel) {
    if (expr != NIL) {
        // Build attribute mapping between relations by column name
        AttrMap *part_attmap = build_attrmap_by_name(RelationGetDescr(to_rel),
                                                    RelationGetDescr(from_rel),
                                                    false);

        // Map variable attribute numbers using the attribute map
        bool found_whole_row;
        expr = (List *) map_variable_attnos((Node *) expr,
                                           fromrel_varno, 0,
                                           part_attmap,
                                           RelationGetForm(to_rel)->reltype,
                                           &found_whole_row);
        // found_whole_row is ignored since we provided to_rowtype
    }

    return expr;
}
```