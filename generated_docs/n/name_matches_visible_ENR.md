# name_matches_visible_ENR

## Location
[src/backend/parser/parse_enr.c:20-25](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_enr.c#L20-L25)

## Overview
Checks whether a given reference name matches a visible Ephemeral Named Relation (ENR) in the current parse state context.

## Definition

```c
bool
name_matches_visible_ENR(ParseState *pstate, const char *refname)
```
## Detailed Description
This function serves as a boolean test to determine if a specified relation name corresponds to a visible ENR within the current parsing environment. It acts as a wrapper around `get_visible_ENR_metadata`, returning true if the ENR exists and is accessible in the current query environment, false otherwise. ENRs are temporary named relations that exist only during query execution, commonly used for CTEs (Common Table Expressions) and other temporary constructs.

## Parameters / Member Variables
- `pstate`: Pointer to the ParseState structure containing the current parsing context and query environment
- `refname`: String containing the name of the relation to search for in the visible ENR list

## Dependencies
- Functions called/Symbols referenced:
  - [get_visible_ENR_metadata](../g/get_visible_ENR_metadata.md)
- Called from (representative examples):
  - [scanNameSpaceForENR](../s/scanNameSpaceForENR.md)

## Notes and Other Information
- This function provides a simple boolean interface for ENR visibility checking
- Returns true if the ENR exists and is visible, false if not found or not visible
- Part of PostgreSQL's parser infrastructure for handling temporary named relations
- Located in src/backend/parser/parse_enr.c:20-25