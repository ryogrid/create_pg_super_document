# findTargetlistEntrySQL99

## Location
src/backend/parser/parse_clause.c: 2172 - 2257

## Overview
Returns the targetlist entry matching a given expression using SQL99 interpretation, where expressions are treated as ordinary expressions referencing input column names rather than output column names or positions.

## Definition


## Detailed Description
This function implements the SQL99 standard interpretation for expressions in ORDER BY, GROUP BY, and similar clauses. Unlike SQL92 interpretation, it treats expressions as ordinary expressions that reference input column names from the FROM clause rather than output column names or positional references from the SELECT list.

The function works by:
1. Converting the untransformed node into a fully transformed expression using transformExpr
2. Searching through the existing target list for an equivalent expression
3. If a match is found, returning that existing TargetEntry (including resjunk entries)
4. If no match exists, creating a new TargetEntry marked as resjunk and appending it to the target list

The matching process ignores implicit casts on existing target list expressions, allowing the ORDER/GROUP clause item to adopt the same datatype as a textually-equivalent target list item.

## Parameters / Member Variables
- : Parse state containing parsing context and transformation information
- : The ORDER BY, GROUP BY, or similar expression to be matched (untransformed)
- : Pointer to the target list (passed by reference so new entries can be appended)
- : Enumeration identifying the clause type being processed

## Dependencies
- Functions called/Symbols referenced:
  - transformExpr
  - strip_implicit_coercions
  - equal
  - transformTargetEntry
  - lappend
  - ParseExprKind
- Called from (representative examples):
  - findTargetlistEntrySQL92
  - transformGroupClauseExpr
  - transformSortClause

## Notes and Other Information
- This is a static function within parse_clause.c for internal parser use
- Implements the modern SQL99 standard behavior for expression matching
- Unlike SQL92 behavior, it allows matching against resjunk target entries
- Creates new resjunk entries when no existing match is found, ensuring the expression can be evaluated even if not explicitly selected
- The strip_implicit_coercions call enables matching expressions that differ only by implicit type conversions
- New target entries are always marked as resjunk=true to prevent them from appearing in the final output unless explicitly selected