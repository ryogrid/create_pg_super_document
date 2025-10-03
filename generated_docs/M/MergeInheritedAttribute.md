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
- `*inh_columns`: List of previously inherited ColumnDef structures from earlier parent processing
- `exist_attno`: Attribute number (1-based) of the existing matching attribute in the inh_columns list
- `*newdef`: New parent column/attribute definition to be merged into the existing one
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

## Simplified Source
```c
static ColumnDef *MergeInheritedAttribute(List *inh_columns, int exist_attno, const ColumnDef *newdef) {
    char *attributeName = newdef->colname;
    ColumnDef *prevdef;
    Oid prevtypeid, newtypeid;
    int32 prevtypmod, newtypmod;
    Oid prevcollid, newcollid;

    // Log merge operation
    ereport(NOTICE, "merging multiple inherited definitions of column \"%s\"", attributeName);
    prevdef = list_nth_node(ColumnDef, inh_columns, exist_attno - 1);

    // Validate type compatibility
    typenameTypeIdAndMod(NULL, prevdef->typeName, &prevtypeid, &prevtypmod);
    typenameTypeIdAndMod(NULL, newdef->typeName, &newtypeid, &newtypmod);
    if (prevtypeid != newtypeid || prevtypmod != newtypmod)
        ereport(ERROR, "inherited column \"%s\" has a type conflict", attributeName);

    // Merge NOT NULL constraints (OR them together)
    prevdef->is_not_null |= newdef->is_not_null;

    // Validate collation compatibility
    prevcollid = GetColumnDefCollation(NULL, prevdef, prevtypeid);
    newcollid = GetColumnDefCollation(NULL, newdef, newtypeid);
    if (prevcollid != newcollid)
        ereport(ERROR, "inherited column \"%s\" has a collation conflict", attributeName);

    // Merge storage parameters
    if (prevdef->storage == 0)
        prevdef->storage = newdef->storage;
    else if (prevdef->storage != newdef->storage)
        ereport(ERROR, "inherited column \"%s\" has a storage parameter conflict", attributeName);

    // Merge compression parameters
    if (prevdef->compression == NULL)
        prevdef->compression = newdef->compression;
    else if (newdef->compression != NULL && strcmp(prevdef->compression, newdef->compression) != 0)
        ereport(ERROR, "column \"%s\" has a compression method conflict", attributeName);

    // Validate generation consistency
    if (prevdef->generated != newdef->generated)
        ereport(ERROR, "inherited column \"%s\" has a generation conflict", attributeName);

    // Increment inheritance count with overflow check
    prevdef->inhcount++;
    if (prevdef->inhcount < 0)
        ereport(ERROR, "too many inheritance parents");

    return prevdef;
}
```