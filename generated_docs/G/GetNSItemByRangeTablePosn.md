# GetNSItemByRangeTablePosn

## Location
[src/backend/parser/parse_relation.c:510-536](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L510-L536)

## Overview
Finds and returns the ParseNamespaceItem corresponding to a given range table index and nesting depth.

## Definition
```c
ParseNamespaceItem *GetNSItemByRangeTablePosn(ParseState *pstate, int varno, int sublevels_up)
```

## Detailed Description
This function locates a specific ParseNamespaceItem within the parser's namespace hierarchy by navigating to the appropriate ParseState level and searching for the namespace item with the matching range table index. The function first traverses up the ParseState hierarchy according to the `sublevels_up` parameter, then searches through the namespace list at that level for an item whose `p_rtindex` matches the provided `varno`.

The function assumes that a matching namespace item must exist and will raise an internal error if none is found, indicating a bug in the parser logic.

## Parameters / Member Variables
- `pstate`: Pointer to the ParseState structure to start the search from
- `varno`: The range table index (RT index) to search for
- `sublevels_up`: Number of parent ParseState levels to traverse before searching (0 means search in current level)

## Dependencies
- Functions called/Symbols referenced:
  - lfirst (list cell access)
  - elog/ERROR (for internal error reporting)
  - Assert (for debugging assertions)
- Types referenced:
  - [ParseNamespaceItem](../P/ParseNamespaceItem.md)
- Called from (representative examples):
  - [coerce_record_to_complex](../c/coerce_record_to_complex.md) (src/backend/parser/parse_coerce.c:1044)
  - [ParseComplexProjection](../P/ParseComplexProjection.md) (src/backend/parser/parse_func.c:1933)
  - [transformMergeStmt](../t/transformMergeStmt.md) (src/backend/parser/parse_merge.c:211)
  - [ExpandRowReference](../E/ExpandRowReference.md) (src/backend/parser/parse_target.c:1446)

## Notes and Other Information
- Function is declared in src/include/parser/parse_relation.h
- Returns a pointer to the found ParseNamespaceItem, never NULL (errors out if not found)
- Used during various parsing operations that need to resolve range table references
- The function performs a linear search through the namespace list
- Internal error indicates a programming bug, not a user input error
- Critical for resolving Var nodes back to their namespace context during parsing
- The `sublevels_up` mechanism supports references to outer query levels in subqueries

## Simplified Source

```c
ParseNamespaceItem *
GetNSItemByRangeTablePosn(ParseState *pstate, int varno, int sublevels_up)
{
    // Navigate up the ParseState hierarchy to the target level
    while (sublevels_up-- > 0) {
        pstate = pstate->parentParseState;
    }

    // Search the namespace list for matching range table index
    foreach(lc, pstate->p_namespace) {
        ParseNamespaceItem *nsitem = (ParseNamespaceItem *) lfirst(lc);

        if (nsitem->p_rtindex == varno)
            return nsitem;
    }

    // Should never reach here - indicates parser bug
    elog(ERROR, "nsitem not found (internal error)");
    return NULL;
}
```