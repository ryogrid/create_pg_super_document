# addRangeTableEntryForCTE

## Location
src/backend/parser/parse_relation.c: 2314 - 2465

## Overview
Creates a range table entry (RTE) for a Common Table Expression (CTE) reference and adds it to the parser state's range table, returning a ParseNamespaceItem for the new CTE entry.

## Definition
```c
ParseNamespaceItem *addRangeTableEntryForCTE(ParseState *pstate,
                                             CommonTableExpr *cte,
                                             Index levelsup,
                                             RangeVar *rv,
                                             bool inFromCl)
```

## Detailed Description
This function creates a RangeTblEntry of type RTE_CTE for handling Common Table Expression references in SQL statements. It manages CTE-specific metadata including self-reference detection, reference counting, and recursive CTE handling. The function validates that data-modifying CTEs (INSERT/UPDATE/DELETE/MERGE) have RETURNING clauses and handles special CTE features like SEARCH and CYCLE clauses that add additional columns. It automatically copies column type information from the CTE definition and manages alias resolution for column names.

## Parameters / Member Variables
- `pstate`: ParseState containing the current parsing context and range table
- `cte`: CommonTableExpr structure containing the CTE definition and metadata
- `levelsup`: Number of nesting levels up to find the CTE definition (0 = current level)
- `rv`: RangeVar containing the reference information and optional alias
- `inFromCl`: Boolean indicating if this appears in the FROM clause

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for RangeTblEntry creation)
  - copyObject (for alias copying)
  - makeAlias (for default alias creation)
  - list_copy (for copying CTE column information)
  - makeString (for column name creation)
  - lappend_oid, lappend_int (for column type management)
  - buildNSItemFromLists (for ParseNamespaceItem construction)
- Called from (representative examples):
  - getNSItemForSpecialRelationTypes (in parse_clause.c:1027)

## Notes and Other Information
- Automatically detects self-references by checking if CTE's parse analysis is completed
- Increments CTE reference count for non-self-referencing uses
- Validates that data-modifying CTEs have RETURNING clauses (except for self-references)
- Handles SEARCH clause by adding a search sequence column (RECORD or RECORDARRAY type)
- Handles CYCLE clause by adding cycle mark and cycle path columns
- SEARCH and CYCLE clause columns are marked as non-expandable in star expansion for nested queries
- Access permissions are not checked for CTE RTEs as they are treated like subqueries
- Self-references are only allowed for recursive CTEs
- Located in src/backend/parser/parse_relation.c:2314-2465