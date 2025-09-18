# errorMissingRTE

## Location
[src/backend/parser/parse_relation.c:3594-3664](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L3594-L3664)

## Overview
Generates detailed error messages when a referenced table or relation is missing from the FROM clause, providing context-specific hints to help users identify and correct the issue.

## Definition


## Detailed Description
This function is responsible for generating helpful error messages when PostgreSQL's parser encounters a reference to a table or relation that cannot be found in the current query's range table. The function performs sophisticated analysis to determine the most likely cause of the error and provides specific hints to guide users toward the correct syntax.

The function implements three levels of error detection and reporting:
1. **Alias confusion detection**: Identifies cases where users reference a table by its real name instead of its alias
2. **Scope violation detection**: Catches attempts to reference tables outside their valid scope (like MySQL-style JOIN syntax)  
3. **Missing table detection**: Handles cases where the table simply doesn't exist in the FROM clause

The function leverages PostgreSQL's namespace resolution system to provide contextually appropriate error messages with precise location information.

## Parameters / Member Variables
- : ParseState structure containing the current parsing context and namespace information
- : RangeVar structure representing the problematic table reference that couldn't be resolved

## Dependencies
- Functions called/Symbols referenced:
  - [searchRangeTableForRel](../s/searchRangeTableForRel.md)
  - [refnameNamespaceItem](../r/refnameNamespaceItem.md)  
  - [rte_visible_if_lateral](../r/rte_visible_if_lateral.md)
  - ereport
  - [errcode](errcode.md) (ERRCODE_UNDEFINED_TABLE)
  - [errmsg](errmsg.md)
  - [errhint](errhint.md)
  - [errdetail](errdetail.md)
  - [parser_errposition](../p/parser_errposition.md)
- Called from (representative examples):
  - Various parser functions when table resolution fails

## Notes and Other Information
- The function works particularly hard to provide helpful messages since missing RTE errors are very common
- Implements special handling for alias-related confusion, which is a frequent user mistake
- Uses location information to provide precise error positioning in the source query
- Supports LATERAL join hint suggestions when appropriate
- Part of PostgreSQL's comprehensive error reporting system that aims to guide users toward correct SQL syntax