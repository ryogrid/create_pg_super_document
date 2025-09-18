# FigureIndexColname

## Location
src/backend/parser/parse_target.c: 1723 - 1742

## Overview
Determines a suitable column name for expression-based index columns, similar to FigureColname but returns NULL when no good name can be determined.

## Definition


## Detailed Description
FigureIndexColname is a specialized variant of FigureColname designed specifically for naming columns in index definitions. While FigureColname always returns a name (defaulting to "?column?" if necessary), FigureIndexColname takes a more conservative approach and returns NULL when a suitable name cannot be determined.

The function uses the same underlying logic as FigureColname by delegating to FigureColnameInternal, but it differs in handling cases where the confidence level is insufficient. This conservative behavior is appropriate for index contexts where having no name is often preferable to having a generic placeholder name.

This function is particularly useful when creating indexes on expressions where PostgreSQL needs to generate internal column names. Unlike SELECT list columns where "?column?" is acceptable for display, index column names should be more meaningful or simply omitted.

The function applies the same sophisticated heuristics as FigureColname:
- Extracts names from column references, function calls, and various expression types
- Uses confidence-based evaluation (strength levels 0-2)
- Returns the derived name only if FigureColnameInternal determines a suitable name exists

## Parameters / Member Variables
- : The untransformed parse tree node representing the index expression from which to derive a column name

## Dependencies
- Functions called/Symbols referenced:
  - [FigureColnameInternal](FigureColnameInternal.md)
- Called from (representative examples):
  - [transformIndexStmt](../t/transformIndexStmt.md)

## Notes and Other Information
- This function is primarily used during CREATE INDEX statement processing for expression-based indexes
- The key difference from FigureColname is the more conservative approach: returns NULL instead of "?column?" when no good name is found
- Shares the same underlying implementation (FigureColnameInternal) with FigureColname, ensuring consistent naming logic
- The NULL return value allows calling code to decide whether to use a generated name, skip naming, or handle the situation differently
- This conservative approach is more appropriate for index contexts where meaningful names are preferred over placeholder names
- The function maintains the same performance characteristics as FigureColname since it uses the same core logic