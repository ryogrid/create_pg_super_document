# scanNameSpaceForENR

## Location
src/backend/parser/parse_relation.c: 335 - 355

## Overview
Searches the query's ephemeral named relation namespace for a relation matching the given unqualified reference name.

## Definition
```c
bool scanNameSpaceForENR(ParseState *pstate, const char *refname)
```

## Detailed Description
This function serves as a wrapper around `name_matches_visible_ENR` to check if a given unqualified reference name matches any ephemeral named relation (ENR) in the current parse state's namespace. Ephemeral named relations are temporary relations that exist only during query execution, such as CTE (Common Table Expression) results or other named subqueries. The function returns a boolean indicating whether such a match was found.

## Parameters / Member Variables
- `pstate`: Pointer to the ParseState structure containing the current parsing context and namespace information
- `refname`: The unqualified reference name to search for in the ephemeral named relation namespace

## Dependencies
- Functions called/Symbols referenced:
  - [name_matches_visible_ENR](../n/name_matches_visible_ENR.md)
- Called from (representative examples):
  - [setTargetTable](setTargetTable.md) (src/backend/parser/parse_clause.c:190)
  - [getNSItemForSpecialRelationTypes](../g/getNSItemForSpecialRelationTypes.md) (src/backend/parser/parse_clause.c:1028)
  - [searchRangeTableForRel](searchRangeTableForRel.md) (src/backend/parser/parse_relation.c:381)

## Notes and Other Information
- This function is part of PostgreSQL's parser namespace resolution mechanism
- It specifically deals with ephemeral named relations, which are distinct from permanent catalog relations
- The function is declared in src/include/parser/parse_relation.h
- Returns true if a matching ENR is found, false otherwise