# get_matching_location

## Location
src/backend/parser/parse_clause.c: 3176 - 3200

## Overview
A utility function that finds the parse location of an expression in the original DISTINCT ON list that corresponds to a specific sort group reference, used for precise error reporting.

## Definition


## Detailed Description
This static helper function is designed specifically to support error reporting in DISTINCT ON clause processing. When PostgreSQL needs to report an error about a problematic DISTINCT ON entry, it must point to the location in the original user query where that expression appeared. However, during query transformation, expressions get moved around and assigned to target list entries that may point to different locations (like matching SELECT list or ORDER BY entries).

The function takes a sort group reference number and searches through parallel lists of sort group references and their corresponding original expressions to find the exact parse location where the user wrote the problematic expression. This enables PostgreSQL to provide precise error messages that point to the correct location in the user's query.

## Parameters / Member Variables
- : The sort group reference number to search for
- : List of integer sort group reference numbers, parallel to exprs list
- : List of original expression nodes from the DISTINCT ON clause, parallel to sortgrouprefs list

## Dependencies
- Functions called/Symbols referenced:
  - forboth: Macro for iterating over two lists simultaneously
  - lfirst_int: Macro to get integer value from current list cell
  - [exprLocation](../e/exprLocation.md): Gets the parse location of an expression node
- Called from (representative examples):
  - [transformDistinctOnClause](../t/transformDistinctOnClause.md): Used when reporting errors about mismatched DISTINCT ON and ORDER BY expressions

## Notes and Other Information
- This is a static function, only used within parse_clause.c
- The function is specifically designed for error reporting, not for normal query processing logic
- Uses the original untransformed DISTINCT ON expressions to get accurate parse locations
- The function includes an elog(ERROR) if no matching sort group reference is found, indicating a programming error in the caller
- The parallel lists (sortgrouprefs and exprs) must be one-to-one corresponding, maintained during DISTINCT ON processing
- Returns -1 as a fallback to keep the compiler quiet, though this should never be reached due to the error call