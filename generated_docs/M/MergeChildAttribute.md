# MergeChildAttribute

## Location
[src/backend/commands/tablecmds.c:3118-3278](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L3118-L3278)

## Overview
A specialized function that merges a child table's explicit column definition with an inherited column definition from a parent table, validating compatibility and resolving conflicts.

## Definition
static void MergeChildAttribute(List *inh_columns, int exist_attno, int newcol_attno, const ColumnDef *newdef)

## Detailed Description
This function handles the complex logic of merging explicitly defined child table columns with inherited parent columns during table inheritance operations. It enforces PostgreSQL's strict inheritance rules while allowing child tables to override certain parent column properties.

The function performs comprehensive validation and merging:

1. **Type Compatibility**: Verifies that child and parent columns have identical data types and type modifiers, preventing type conflicts that would break inheritance semantics

2. **Collation Consistency**: Ensures collation settings match between parent and child columns for proper string comparison behavior

3. **Storage Parameter Merging**: Handles storage specifications (PLAIN, EXTERNAL, EXTENDED, MAIN) with preference for child definitions when compatible

4. **Compression Method Validation**: Checks compression method compatibility and resolves conflicts

5. **Generated Column Logic**: Implements complex rules for generated columns - children can override generation expressions but must maintain generated status consistency with parents

6. **Default Value Override**: Allows child columns to specify default values that override inherited defaults

7. **NOT NULL Constraint Union**: Combines NOT NULL constraints using logical OR (if either parent or child specifies NOT NULL, the result is NOT NULL)

The function modifies the inherited column definition in place and handles proper positioning when child column definitions are specified in different order than inheritance hierarchy.

## Parameters / Member Variables
- inh_columns: List of inherited ColumnDef structures representing parent table columns
- exist_attno: Attribute number of the existing inherited column (1-based indexing)
- newcol_attno: Attribute number of the child column in the table schema definition
- newdef: ColumnDef structure representing the child table's explicit column definition

## Dependencies
- Functions called/Symbols referenced:
  - [typenameTypeIdAndMod](../t/typenameTypeIdAndMod.md) (resolves type names to OIDs and modifiers)
  - [GetColumnDefCollation](../G/GetColumnDefCollation.md) (determines column collation settings)
  - [format_type_with_typemod](../f/format_type_with_typemod.md) (formats type information for error messages)
  - [get_collation_name](../g/get_collation_name.md) (retrieves collation names for error messages)
  - [storage_name](../s/storage_name.md) (converts storage type codes to names for error messages)
  - list_nth_node (retrieves specific list elements)
  - ereport (error and notice reporting)
- Called from (representative examples):
  - [MergeAttributes](MergeAttributes.md) (main attribute merging function during table creation)

## Notes and Other Information
- This is a static function within tablecmds.c, used exclusively during table inheritance processing
- Only applicable to regular inheritance (not partitioning) as partitions cannot have their own column definitions
- Generates NOTICE messages when merging columns to inform users of the operation
- Implements PostgreSQL's principle that inheritance should maintain strict type safety while allowing property overrides
- The function is destructive - it modifies the inherited column definition rather than creating a new one
- Critical for maintaining data integrity and type safety across inheritance hierarchies

## Simplified Source
```c
static void MergeChildAttribute(List *inh_columns, int exist_attno, int newcol_attno, const ColumnDef *newdef) {
    char *attributeName = newdef->colname;
    ColumnDef *inhdef;
    Oid inhtypeid, newtypeid;
    int32 inhtypmod, newtypmod;
    Oid inhcollid, newcollid;

    // Log merge operation notice
    if (exist_attno == newcol_attno)
        ereport(NOTICE, "merging column \"%s\" with inherited definition", attributeName);
    else
        ereport(NOTICE, "moving and merging column \"%s\" with inherited definition", attributeName);

    inhdef = list_nth_node(ColumnDef, inh_columns, exist_attno - 1);

    // Validate type compatibility
    typenameTypeIdAndMod(NULL, inhdef->typeName, &inhtypeid, &inhtypmod);
    typenameTypeIdAndMod(NULL, newdef->typeName, &newtypeid, &newtypmod);
    if (inhtypeid != newtypeid || inhtypmod != newtypmod)
        ereport(ERROR, "column \"%s\" has a type conflict", attributeName);

    // Validate collation compatibility
    inhcollid = GetColumnDefCollation(NULL, inhdef, inhtypeid);
    newcollid = GetColumnDefCollation(NULL, newdef, newtypeid);
    if (inhcollid != newcollid)
        ereport(ERROR, "column \"%s\" has a collation conflict", attributeName);

    // Copy identity setting (child takes precedence)
    inhdef->identity = newdef->identity;

    // Merge storage parameters
    if (inhdef->storage == 0)
        inhdef->storage = newdef->storage;
    else if (newdef->storage != 0 && inhdef->storage != newdef->storage)
        ereport(ERROR, "column \"%s\" has a storage parameter conflict", attributeName);

    // Merge compression settings
    if (inhdef->compression == NULL)
        inhdef->compression = newdef->compression;
    else if (newdef->compression != NULL && strcmp(inhdef->compression, newdef->compression) != 0)
        ereport(ERROR, "column \"%s\" has a compression method conflict", attributeName);

    // Merge NOT NULL constraints (OR them together)
    inhdef->is_not_null |= newdef->is_not_null;

    // Validate generated column rules
    if (inhdef->generated) {
        if (newdef->raw_default && !newdef->generated)
            ereport(ERROR, "column \"%s\" inherits from generated column but specifies default", inhdef->colname);
        if (newdef->identity)
            ereport(ERROR, "column \"%s\" inherits from generated column but specifies identity", inhdef->colname);
    } else {
        if (newdef->generated)
            ereport(ERROR, "child column \"%s\" specifies generation expression", inhdef->colname);
    }

    // Override default value if child specifies one
    if (newdef->raw_default != NULL) {
        inhdef->raw_default = newdef->raw_default;
        inhdef->cooked_default = newdef->cooked_default;
    }

    // Mark column as locally defined
    inhdef->is_local = true;
}
```