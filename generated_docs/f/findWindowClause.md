# findWindowClause

## Location
src/backend/parser/parse_clause.c: 3659 - 3685

## Overview
Locates a named WindowClause in a list of window clauses by name, returning the clause or NULL if not found.

## Definition


## Detailed Description
This simple utility function performs a linear search through a list of WindowClause structures to find one with a matching name. It's used during window function processing to resolve window clause references by name in SQL window specifications.

The function iterates through the provided list, comparing each WindowClause's name field against the target name using string comparison. It returns the first matching WindowClause or NULL if no match is found.

## Parameters / Member Variables
- : List of WindowClause structures to search through
- : Name of the window clause to find (string)

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (standard C library function)
- Called from (representative examples):
  - [transformWindowDefinitions](../t/transformWindowDefinitions.md) (called twice in same function)

## Notes and Other Information
- Static function internal to parse_clause.c for window function processing
- Performs case-sensitive string matching using strcmp
- Returns NULL if the target name is not found in the list
- Simple linear search algorithm - suitable for typical small window clause lists
- Used during parsing of SQL window functions and OVER clauses
- Critical for resolving named window specifications in complex window function queries
- The function safely handles NULL window clause names by checking wc->name before comparison