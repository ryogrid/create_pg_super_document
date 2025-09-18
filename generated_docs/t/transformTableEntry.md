# transformTableEntry

## Location
src/backend/parser/parse_clause.c: 397 - 406

## Overview
Transforms a RangeVar (simple relation reference) into a ParseNamespaceItem by delegating to addRangeTableEntry with appropriate parameters.

## Definition


## Detailed Description
The transformTableEntry function is a wrapper function that simplifies the transformation of a simple table reference (RangeVar) into a ParseNamespaceItem. It serves as an intermediary in the SQL parsing pipeline, specifically handling the transformation of basic table references in the FROM clause. The function extracts the necessary information from the RangeVar structure and passes it to addRangeTableEntry, which performs the actual work of adding the table to the range table and creating the corresponding ParseNamespaceItem.

## Parameters / Member Variables
- : ParseState structure containing the current parsing context and state information
- : RangeVar structure representing the simple relation reference to be transformed

## Dependencies
- Functions called/Symbols referenced:
  - [addRangeTableEntry](../a/addRangeTableEntry.md)
  - [RangeVar](../R/RangeVar.md) (struct type)
  - [ParseNamespaceItem](../P/ParseNamespaceItem.md) (struct type)
- Called from (representative examples):
  - [transformFromClauseItem](transformFromClauseItem.md)

## Notes and Other Information
- This is a static function within parse_clause.c, indicating it's only used internally within that compilation unit
- The function passes r->alias, r->inh (inheritance flag), and true (for the visible parameter) to addRangeTableEntry
- This function represents the simplest case of range table entry transformation, handling basic table references without subqueries, functions, or other complex constructs
- The delegation pattern used here allows for clean separation of concerns between different types of range table transformations