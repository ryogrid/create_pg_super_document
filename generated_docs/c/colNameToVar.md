# colNameToVar

## Location
[src/backend/parser/parse_relation.c:883-951](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L883-L951)

## Overview
Searches for an unqualified column name in the parser namespace and returns the appropriate Var node or expression if found.

## Definition

```c
Node *
colNameToVar(ParseState *pstate, const char *colname, bool localonly,
			 int location)
```
## Detailed Description
The  function performs an unqualified column name lookup within the PostgreSQL parser. It searches through the parser namespace hierarchy, starting from the current parse state and potentially traversing up to parent parse states. The function handles ambiguity detection by raising an error if the same column name is found in multiple namespace items. It also respects lateral reference rules and visibility constraints for namespace items.

The search process iterates through all namespace items in the current parse state, filtering out items that are not column-visible or lateral-only items when not in a lateral context. For each valid namespace item, it calls  to perform the actual column search.

## Parameters / Member Variables
- : The current parse state containing the namespace to search
- : The unqualified column name to search for
- : If true, only search in the innermost query level (don't traverse parent parse states)
- : Source location for error reporting purposes

## Dependencies
- Functions called/Symbols referenced:
  - [scanNSItemForColumn](../s/scanNSItemForColumn.md)
  - [check_lateral_ref_ok](check_lateral_ref_ok.md)
  - [ParseNamespaceItem](../P/ParseNamespaceItem.md)
  - [FuzzyAttrMatchState](../F/FuzzyAttrMatchState.md)
- Called from (representative examples):
  - [findTargetlistEntrySQL92](../f/findTargetlistEntrySQL92.md)
  - [CRERR_TOO_MANY](../C/CRERR_TOO_MANY.md)

## Notes and Other Information
- Returns NULL if the column name is not found
- Raises an ERROR with ERRCODE_AMBIGUOUS_COLUMN if the column name is ambiguous
- Handles lateral reference validation through check_lateral_ref_ok
- The function maintains consistency by using the original parse state for scanNSItemForColumn calls
- Supports hierarchical namespace searching unless localonly is specified

## Simplified Source

```c
Node *
colNameToVar(ParseState *pstate, const char *colname, bool localonly,
             int location)
{
    Node *result = NULL;
    int sublevels_up = 0;
    ParseState *orig_pstate = pstate;

    // Search through parse state hierarchy
    while (pstate != NULL)
    {
        ListCell *l;

        // Check each namespace item in current parse state
        foreach(l, pstate->p_namespace)
        {
            ParseNamespaceItem *nsitem = (ParseNamespaceItem *) lfirst(l);
            Node *newresult;

            // Skip items that aren't column-visible
            if (!nsitem->p_cols_visible)
                continue;
            // Skip lateral-only items when not in lateral context
            if (nsitem->p_lateral_only && !pstate->p_lateral_active)
                continue;

            // Search for column in this namespace item
            newresult = scanNSItemForColumn(orig_pstate, nsitem, sublevels_up,
                                            colname, location);

            if (newresult)
            {
                // Check for ambiguous column reference
                if (result)
                    ereport(ERROR,
                            (errcode(ERRCODE_AMBIGUOUS_COLUMN),
                             errmsg("column reference \"%s\" is ambiguous",
                                    colname),
                             parser_errposition(pstate, location)));

                // Validate lateral reference if applicable
                check_lateral_ref_ok(pstate, nsitem, location);
                result = newresult;
            }
        }

        // Stop if found or only searching locally
        if (result != NULL || localonly)
            break;

        // Move up to parent parse state
        pstate = pstate->parentParseState;
        sublevels_up++;
    }

    return result;
}
```