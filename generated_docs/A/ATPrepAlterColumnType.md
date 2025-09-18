# ATPrepAlterColumnType

## Location
src/backend/commands/tablecmds.c: 12807 - 13098

## Overview
Prepares ALTER COLUMN TYPE operations during Phase 1 of ALTER TABLE processing, handling type validation, expression transformation, and inheritance recursion.

## Definition


## Detailed Description
This function performs Phase 1 preparation for ALTER COLUMN TYPE operations. Unlike other ALTER TABLE subcommands, it performs parse transformation during Phase 1 to ensure all USING expressions are parsed against the original table schema. The function validates the target column exists and is alterable, checks type compatibility and permissions, transforms USING expressions or creates default coercion expressions, determines if a table rewrite is required, and handles inheritance recursion with proper attribute number remapping. It supports both regular tables and typed tables, with special handling for generated columns, partition keys, and inherited columns.

## Parameters / Member Variables
- : Work queue for queueing additional ALTER TABLE commands
- : Information about the table being altered
- : The relation being altered
- : Whether to recursively process child tables
- : True when called recursively on child tables
- : The ALTER TABLE command containing column and type information
- : Lock level to use when accessing child relations
- : Utility context for additional ALTER TABLE processing

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheAttName (column lookup)
  - has_partition_attrs (partition key validation)
  - typenameTypeIdAndMod (type resolution)
  - object_aclcheck/aclcheck_error_type (permission checking)
  - GetColumnDefCollation (collation handling)
  - CheckAttributeType (type validation)
  - coerce_to_target_type (type coercion)
  - assign_expr_collations (expression processing)
  - expression_planner (expression optimization)
  - ATColumnChangeRequiresRewrite (rewrite determination)
  - find_all_inheritors (inheritance processing)
  - build_attrmap_by_name/map_variable_attnos (attribute mapping)
  - ATTypedTableRecursion (typed table handling)
- Called from (representative examples):
  - ATPrepCmd (main ALTER TABLE preparation)

## Notes and Other Information
- Performs parse transformation during Phase 1 to handle USING expressions correctly
- USING expressions are parsed against the original table schema before any alterations
- Cannot alter system columns, inherited columns (at top level), or generated columns with USING
- Prevents altering columns used in partition keys
- For tables requiring rewrite, creates NewColumnValue entries for ATRewriteTable
- Uses custom recursion mechanism instead of ATSimpleRecursion for attribute remapping
- Handles both explicit USING clauses and automatic type coercion
- Supports typed tables through ATTypedTableRecursion
- Must execute after AT_PASS_DROP in Phase 2 to see unmodified table state