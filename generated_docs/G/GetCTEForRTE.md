# GetCTEForRTE

## Location
src/backend/parser/parse_relation.c: 557 - 586

## Overview
Retrieves the Common Table Expression (CTE) definition corresponding to a CTE-reference Range Table Entry, resolving it across multiple query nesting levels.

## Definition


## Detailed Description
This function locates and returns the CommonTableExpr structure that defines a CTE referenced by a given Range Table Entry. It navigates through potentially nested ParseState contexts to find the appropriate query level where the CTE was defined, then searches the CTE namespace at that level to find the matching CTE by name. The function handles the complex scoping rules of CTEs in PostgreSQL, where a CTE defined at an outer query level can be referenced by inner subqueries.

## Parameters / Member Variables
- : ParseState pointer representing the current parser state context
- : RangeTblEntry pointer that must be of type RTE_CTE (CTE reference)
- : Number of query levels above the current pstate where the RTE was found

## Dependencies
- Functions called/Symbols referenced:
  - RTE_CTE (constant for RTE kind checking)
  - CommonTableExpr (structure type)
- Called from (representative examples):
  - markTargetListOrigin
  - expandRecordVariable

## Notes and Other Information
- Function includes assertion to ensure the RTE is of type RTE_CTE
- Combines rte->ctelevelsup with rtelevelsup to determine total levels to traverse
- Searches CTE namespace by string comparison of CTE names
- Includes error handling for cases where CTE cannot be found or invalid nesting levels
- Essential for resolving CTE references during query parsing and analysis
- Located in src/backend/parser/parse_relation.c:557-586