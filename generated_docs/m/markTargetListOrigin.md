# markTargetListOrigin

## Location
[src/backend/parser/parse_target.c:343-451](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_target.c#L343-L451)

## Overview
Marks a TargetEntry with the origin table and column information if the referenced variable is from a plain relation, enabling column provenance tracking in PostgreSQL query processing.

## Definition

```c
static void
markTargetListOrigin(ParseState *pstate, TargetEntry *tle,
					 Var *var, int levelsup)
```
## Detailed Description
This function analyzes a Var node to determine its origin table and column, then marks the corresponding TargetEntry with this provenance information. It handles various types of range table entries including base relations, subqueries, and CTEs (Common Table Expressions). The function does not drill down into views but reports the view as the column owner. For joins, it only processes merged JOIN USING columns and whole-row variables, not drilling down to constituent table columns.

The function performs different actions based on the RTE (Range Table Entry) type:
- For base relations: directly sets origin table and column
- For subqueries: recursively copies origin information from the subquery's target list
- For CTEs: copies origin information while handling recursive self-references and search/cycle columns
- For other RTE types (joins, functions, values, etc.): leaves the entry unmarked

## Parameters / Member Variables
- : Parse state containing context for the current query parsing
- : Target entry to be marked with origin information
- : Variable node to analyze for origin determination
- : Extra offset to correctly interpret the variable's varlevelsup for nested contexts

## Dependencies
- Functions called/Symbols referenced:
  - [GetRTEByRangeTablePosn](../G/GetRTEByRangeTablePosn.md)
  - [get_tle_by_resno](../g/get_tle_by_resno.md)
  - [GetCTEForRTE](../G/GetCTEForRTE.md)
  - GetCTETargetList
  - Constants: RTE_RELATION, RTE_SUBQUERY, RTE_JOIN, RTE_FUNCTION, RTE_VALUES, RTE_TABLEFUNC, RTE_NAMEDTUPLESTORE, RTE_RESULT, RTE_CTE, InvalidAttrNumber
- Called from:
  - [markTargetListOrigins](markTargetListOrigins.md)

## Notes and Other Information
- The function is static and only used within parse_target.c
- Handles special cases for CTE search and cycle columns that are added to RTEs but not present in the original subquery
- Does not process recursive CTE self-references to avoid incomplete analysis
- Returns early if the input variable is NULL or not a Var node
- Critical for implementing column-level security and query optimization features that depend on column provenance