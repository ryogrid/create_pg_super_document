# MergeChildAttribute

## Location
src/backend/commands/tablecmds.c: 3118 - 3278

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
  - typenameTypeIdAndMod (resolves type names to OIDs and modifiers)
  - GetColumnDefCollation (determines column collation settings)
  - format_type_with_typemod (formats type information for error messages)
  - get_collation_name (retrieves collation names for error messages)
  - storage_name (converts storage type codes to names for error messages)
  - list_nth_node (retrieves specific list elements)
  - ereport (error and notice reporting)
- Called from (representative examples):
  - MergeAttributes (main attribute merging function during table creation)

## Notes and Other Information
- This is a static function within tablecmds.c, used exclusively during table inheritance processing
- Only applicable to regular inheritance (not partitioning) as partitions cannot have their own column definitions
- Generates NOTICE messages when merging columns to inform users of the operation
- Implements PostgreSQL's principle that inheritance should maintain strict type safety while allowing property overrides
- The function is destructive - it modifies the inherited column definition rather than creating a new one
- Critical for maintaining data integrity and type safety across inheritance hierarchies