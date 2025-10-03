# transformTableEntry

## Location
[src/backend/parser/parse_clause.c:397-406](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L397-L406)

## Overview
Transforms a RangeVar (simple relation reference) into a ParseNamespaceItem by delegating to addRangeTableEntry with appropriate parameters.

## Definition

```c
static ParseNamespaceItem *
transformTableEntry(ParseState *pstate, RangeVar *r)
```
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

## Simplified Source

```c
static ParseNamespaceItem *
transformTableEntry(ParseState *pstate, RangeVar *r)
{
    // Transform simple table reference by delegating to addRangeTableEntry
    // Pass alias, inheritance flag, and visibility=true
    return addRangeTableEntry(pstate, r, r->alias, r->inh, true);
}
```