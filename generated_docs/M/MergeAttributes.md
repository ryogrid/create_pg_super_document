# MergeAttributes

## Location
src/backend/commands/tablecmds.c: 2470 - 3051

## Overview
A comprehensive function that merges column definitions from parent tables with explicitly declared columns to create a complete schema for table inheritance or partitioning.

## Definition
static List *MergeAttributes(List *columns, const List *supers, char relpersistence, bool is_partition, List **supconstr)

## Detailed Description
This function is the core engine for PostgreSQL's table inheritance and partitioning system. It takes column definitions from multiple parent tables and merges them with any explicitly declared columns to produce a unified schema. The function handles complex inheritance scenarios including:

1. **Multi-level inheritance**: Processes parent tables in left-to-right order, preserving inheritance hierarchy
2. **Column conflict resolution**: Merges columns with identical names, ensuring type compatibility and resolving default value conflicts
3. **Constraint inheritance**: Inherits CHECK constraints from parents, adjusting column references appropriately
4. **Default value handling**: Implements sophisticated rules for inheriting and overriding default values and generation expressions
5. **Partition-specific logic**: Special handling for partitioned tables with different validation rules

The function maintains PostgreSQL's inheritance semantics where attributes appear in the order of first definition across the inheritance hierarchy. It validates persistence constraints, ownership permissions, and relationship types throughout the process.

## Parameters / Member Variables
- columns: List of ColumnDef structures representing explicitly declared columns (destructively modified)
- supers: List of OIDs representing parent relation identifiers (already locked by caller)
- relpersistence: Character indicating table persistence type (permanent, temporary, etc.)
- is_partition: Boolean flag indicating whether this is a partition operation vs regular inheritance
- supconstr: Output parameter receiving list of inherited constraints from parents

## Dependencies
- Functions called/Symbols referenced:
  - makeColumnDef (creates column definition structures)
  - findAttrByName (locates columns by name in inheritance hierarchy)
  - MergeInheritedAttribute (merges column definitions from multiple parents)
  - MergeChildAttribute (merges child and inherited column definitions)
  - MergeCheckConstraint (merges inherited CHECK constraints)
  - make_attrmap/free_attrmap (manages attribute number mapping)
  - map_variable_attnos (adjusts variable references in expressions)
  - CheckTableNotInUse (validates table availability for operations)
- Called from (representative examples):
  - DefineRelation (main table creation function)

## Notes and Other Information
- This is a static function within tablecmds.c, central to PostgreSQL's DDL processing
- The function is destructive - it modifies the input columns list during processing
- Implements PostgreSQL's complex inheritance rules including attribute ordering, default value precedence, and constraint merging
- Handles both regular inheritance and partitioning with different validation rules for each
- Performs extensive validation including column limits (MaxHeapAttributeNumber), type compatibility, and permission checks
- The algorithm is O(n^2) in column count but optimized for typical table sizes
- Critical for maintaining PostgreSQL's inheritance semantics and ensuring schema consistency across table hierarchies