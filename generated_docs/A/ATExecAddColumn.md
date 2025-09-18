# ATExecAddColumn

## Location
src/backend/commands/tablecmds.c: 7012 - 7437

## Overview
Executes the actual addition of a column to a table, handling inheritance, type validation, default values, and catalog updates while managing complex scenarios like column merging and recursion.

## Definition
```c
static ObjectAddress ATExecAddColumn(List **wqueue, AlteredTableInfo *tab, Relation rel,
                                    AlterTableCmd **cmd, bool recurse, bool recursing,
                                    LOCKMODE lockmode, AlterTablePass cur_pass, 
                                    AlterTableUtilityContext *context)
```

## Detailed Description
This comprehensive function implements the core logic for adding columns to PostgreSQL tables. It handles numerous complex scenarios including inheritance hierarchies, type validation, default value processing, and catalog maintenance. The function operates in multiple phases and includes sophisticated logic for column merging in inheritance scenarios.

Key operations performed:
1. **Inheritance handling**: When adding to child tables, checks for existing columns with matching names and validates type compatibility, merging inheritance counts when appropriate
2. **Validation**: Prevents adding columns to partitions directly, enforces column name uniqueness, and validates type compatibility
3. **Catalog updates**: Updates pg_class and pg_attribute system catalogs with new column information
4. **Default value processing**: Handles various default value scenarios including identity columns, domain constraints, and missing value optimization
5. **Recursion management**: Recursively processes inheritance children while maintaining proper inheritance counts and avoiding infinite loops
6. **Dependency management**: Establishes proper dependencies for data types and collations

The function includes an optimization to avoid full table rewrites when possible by using PostgreSQL's "missing values" feature for default values that can be stored separately from table data.

## Parameters / Member Variables
- `wqueue`: Pointer to the ALTER TABLE work queue for managing related operations
- `tab`: Information about the table being altered, including rewrite requirements
- `rel`: The relation being modified
- `cmd`: Pointer to the ALTER TABLE command (may be modified during processing)
- `recurse`: Whether to apply changes to inheritance children
- `recursing`: Whether this is a recursive call (affects permission checks)
- `lockmode`: Lock mode to use for child relations
- `cur_pass`: Current phase of ALTER TABLE processing
- `context`: Context for command transformation and validation

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth
  - ATSimplePermissions
  - SearchSysCacheCopyAttName
  - typenameTypeIdAndMod
  - GetColumnDefCollation
  - CatalogTupleUpdate
  - CommandCounterIncrement
  - check_for_column_name_collision
  - ATParseTransformCmd
  - BuildDescForRelation
  - CheckAttributeType
  - InsertPgAttributeTuples
  - build_column_default
  - DomainHasConstraints
  - add_column_datatype_dependency
  - find_inheritance_children
  - ATGetQueueEntry
  - CheckAlterTableIsSafe
- Called from (representative examples):
  - ATExecCmd
  - ATExecAddColumn (recursive calls)

## Notes and Other Information
- The function includes stack depth checking to prevent stack overflow during deep recursion
- Column merging logic ensures that inheritance counts are properly maintained across the hierarchy
- Identity columns have special handling and cannot be added recursively to tables with regular inheritance children
- The missing values optimization can avoid expensive table rewrites for non-volatile default expressions
- Partitions inherit identity columns but regular inheritance children do not
- The function maintains transactional consistency through appropriate use of CommandCounterIncrement()
- Error handling provides detailed messages for various failure scenarios including type mismatches and collation conflicts