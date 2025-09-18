# transformValuesClause

## Location
src/backend/parser/analyze.c: 1480 - 1698

## Overview
Transforms a VALUES clause used as a standalone SELECT statement into a Query tree, treating it as if it were "SELECT * FROM (VALUES ...) AS "*VALUES*"".

## Definition


## Detailed Description
transformValuesClause handles the transformation of VALUES clauses that appear as standalone SELECT statements (not within INSERT or other contexts). The function creates a virtual range table entry (RTE) containing the VALUES data and builds a Query structure that selects from this RTE.

The transformation process involves several key steps: First, it validates that only VALUES-specific clauses are present (no FROM, WHERE, GROUP BY, etc.). Then it processes each row of VALUES data, transforming expressions and ensuring all rows have the same number of columns. The function performs type resolution to find common types across all rows in each column, coercing expressions to these common types. It also determines common type modifiers and collations for each column.

The intermediate representation is organized by columns rather than rows to simplify type processing, then reorganized back to row format for the final RTE. The function handles special cases like NEW/OLD references within CREATE RULE contexts by marking the RTE as LATERAL when necessary.

## Parameters / Member Variables
- : ParseState structure containing parsing context and namespace information
- : SelectStmt node representing the VALUES clause to be transformed

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (Query creation)
  - [transformWithClause](transformWithClause.md) (WITH clause processing)
  - [transformExpressionList](transformExpressionList.md) (expression transformation for each VALUES row)
  - [select_common_type](../s/select_common_type.md) (type resolution across columns)
  - [coerce_to_common_type](../c/coerce_to_common_type.md) (type coercion)
  - [select_common_typmod](../s/select_common_typmod.md)/select_common_collation (type modifier and collation resolution)
  - [contain_vars_of_level](../c/contain_vars_of_level.md) (variable reference detection)
  - [addRangeTableEntryForValues](../a/addRangeTableEntryForValues.md) (VALUES RTE creation)
  - [addNSItemToQuery](../a/addNSItemToQuery.md) (namespace item addition)
  - [expandNSItemAttrs](../e/expandNSItemAttrs.md) (target list generation)
  - [transformSortClause](transformSortClause.md)/transformLimitClause (ORDER BY and LIMIT processing)
  - [assign_query_collations](../a/assign_query_collations.md) (collation assignment)
- Called from (representative examples):
  - [transformStmt](transformStmt.md) (main statement transformation dispatcher)

## Notes and Other Information
- The function asserts that incompatible SELECT clauses (DISTINCT, INTO, FROM, WHERE, GROUP BY, HAVING, WINDOW) are not present
- All VALUES rows must have the same length after expression transformation (which may expand * expressions)
- Type resolution is performed column-wise to find common types, with all expressions in each column coerced to the common type
- The function supports ORDER BY and LIMIT clauses on VALUES but rejects FOR UPDATE/SHARE clauses
- LATERAL marking is applied when the VALUES expressions contain references to outer query variables (typically in CREATE RULE contexts)
- Memory optimization includes releasing intermediate sublists to save memory during processing
- The final Query structure appears as if selecting all columns from a virtual table containing the VALUES data