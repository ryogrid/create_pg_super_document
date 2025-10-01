# setNamespaceColumnVisibility

## Location
[src/backend/parser/parse_clause.c:1815-1831](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L1815-L1831)

## Overview
A convenience subroutine that updates the column visibility flags in a namespace list to control whether columns are visible during parsing operations.

## Definition

```c
static void
setNamespaceColumnVisibility(List *namespace, bool cols_visible)
```
## Detailed Description
This function is a utility routine used within the PostgreSQL parser to modify the column visibility state of all namespace items in a given list. It iterates through each ParseNamespaceItem in the provided namespace list and sets the  flag to the specified boolean value. This functionality is crucial for controlling name resolution behavior in different parsing contexts, particularly when handling FROM clause items where column visibility needs to be managed based on the specific SQL construct being processed.

## Parameters / Member Variables
- : A List of ParseNamespaceItem structures representing the current parsing namespace
- : A boolean flag indicating whether columns should be visible (true) or hidden (false) in the namespace

## Dependencies
- Functions called/Symbols referenced:
  - [ParseNamespaceItem](../P/ParseNamespaceItem.md) (struct type)
  - lfirst (list access macro)
  - foreach (list iteration macro)
- Called from (representative examples):
  - [transformFromClauseItem](../t/transformFromClauseItem.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the parse_clause.c file
- The function is primarily used to manage column visibility during FROM clause processing
- Column visibility control is essential for proper SQL name resolution and preventing ambiguous column references
- The function modifies the namespace items in-place rather than creating new copies

## Simplified Source

```c
static void
setNamespaceColumnVisibility(List *namespace, bool cols_visible)
{
    ListCell *lc;

    // Update visibility flag for all namespace items
    foreach(lc, namespace)
    {
        ParseNamespaceItem *nsitem = (ParseNamespaceItem *) lfirst(lc);
        nsitem->p_cols_visible = cols_visible;
    }
}
```