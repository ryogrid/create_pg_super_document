# MergeInheritedAttribute

## Location
[src/backend/commands/tablecmds.c:3279-3388](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L3279-L3388)

## Overview
MergeInheritedAttribute merges a parent attribute definition into an existing inherited attribute from previous parents, ensuring type compatibility and resolving conflicts during table inheritance.

## Definition

```c
static ColumnDef *
MergeInheritedAttribute(List *inh_columns,
						int exist_attno,
						const ColumnDef *newdef)
```
## Detailed Description
This function is a core component of PostgreSQL's table inheritance mechanism. It handles the complex task of merging attribute definitions when a child table inherits from multiple parents that have columns with the same name. The function performs strict validation to ensure that inherited attributes are compatible and follows PostgreSQL's inheritance rules.

Key validation steps include:
- Type and type modifier compatibility checking
- Collation consistency verification
- Storage parameter conflict resolution
- Compression method compatibility
- Generation constraint validation
- NOT NULL constraint merging (using OR logic)

The function modifies the existing ColumnDef in the inheritance list and increments its inheritance count, making it applicable only to regular inheritance (not partitioning).

## Parameters / Member Variables
- : List of previously inherited ColumnDef structures from earlier parent processing
- : Attribute number (1-based) of the existing matching attribute in the inh_columns list
- : New parent column/attribute definition to be merged into the existing one

## Dependencies
- Functions called/Symbols referenced:
  - list_nth_node
  - [typenameTypeIdAndMod](../t/typenameTypeIdAndMod.md)
  - [format_type_with_typemod](../f/format_type_with_typemod.md)
  - [GetColumnDefCollation](../G/GetColumnDefCollation.md)
  - [get_collation_name](../g/get_collation_name.md)
  - [storage_name](../s/storage_name.md)
  - [ColumnDef](../C/ColumnDef.md) (structure type)
- Called from (representative examples):
  - [MergeAttributes](MergeAttributes.md)

## Notes and Other Information
- Only applicable to regular table inheritance, not partitioning (partitions inherit from single parent)
- Issues NOTICE messages when merging multiple inherited definitions of the same column
- Throws specific errors for type conflicts, collation mismatches, storage parameter conflicts, and generation conflicts
- The inhcount field tracks inheritance depth and has overflow protection
- Default constraints and other constraint handling is delegated to the caller function