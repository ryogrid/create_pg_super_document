# create_ctas_nodata

## Location
src/backend/commands/createas.c: 153 - 220

## Overview
Creates a CTAS (CREATE TABLE AS) or materialized view relation when the WITH NO DATA option is specified, deriving column definitions from the SELECT statement's target list.

## Definition


## Detailed Description
The  function handles the creation of relations for CREATE TABLE AS and CREATE MATERIALIZED VIEW statements when no data is to be initially populated (WITH NO DATA clause). It processes the target list from the query to derive appropriate column definitions, including data types, type modifiers, and collations.

The function iterates through non-junk entries in the target list, creating  structures for each. If explicit column names were provided in the CREATE statement, they override the names derived from the query. The function performs validation to ensure collation information is properly resolved for collatable types and that the number of specified column names matches the query's output columns.

Once the column definitions are prepared, it delegates to  for the actual relation creation.

## Parameters / Member Variables
- : List of  nodes representing the SELECT statement's target list from which column definitions are derived
- :  containing the target relation specification, column name overrides, and other creation options

## Dependencies
- Functions called/Symbols referenced:
  - list_head
  - makeColumnDef
  - exprType
  - exprTypmod
  - exprCollation
  - type_is_collatable
  - create_ctas_internal
- Called from (representative examples):
  - DR_intorel
  - ExecCreateTableAs

## Notes and Other Information
- This is a static function within createas.c, used specifically for WITH NO DATA scenarios
- Performs thorough validation of collation information to prevent runtime issues
- Supports column name override through the  list in the 
- Filters out junk entries from the target list (typically used for sorting/grouping)
- Generates appropriate error messages when column name count mismatches occur
- Acts as a preprocessing step before calling the main relation creation logic