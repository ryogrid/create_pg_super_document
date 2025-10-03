# addTargetToGroupList

## Location
[src/backend/parser/parse_clause.c:3536-3590](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L3536-L3590)

## Overview
Adds a target list entry to a SortGroupClause list for grouping operations if not already present, using default sort/group semantics.

## Definition

```c
static List *
addTargetToGroupList(ParseState *pstate, TargetEntry *tle,
					 List *grouplist, List *targetlist, int location)
```
## Detailed Description
This static function is similar to addTargetToSortList but specifically designed for GROUP BY clause processing. It differs in that it only requires a grouping (equality) operator and considers a target entry "already in the list" if it appears with any sorting semantics. The function ensures that each grouping expression appears only once in the group list.

The function performs the same type coercion for UNKNOWN literals as addTargetToSortList and uses default sort/group semantics. It creates SortGroupClause nodes with equality operators required for grouping, optional sort operators, and hashability information for optimization purposes.

## Parameters / Member Variables
- `*pstate`: Parse state containing context information for query parsing
- `*tle`: Target entry to be added to the group list
- `*grouplist`: Current list of SortGroupClause nodes for grouping
- `*targetlist`: Complete target list for the query
- `location`: Parse location for error reporting (cannot rely on tle->expr location)
## Dependencies
- Functions called/Symbols referenced:
  - [coerce_type](../c/coerce_type.md)
  - [targetIsInSortList](../t/targetIsInSortList.md)
  - [setup_parser_errposition_callback](../s/setup_parser_errposition_callback.md)
  - [get_sort_group_operators](../g/get_sort_group_operators.md)
  - [assignSortGroupRef](assignSortGroupRef.md)
- Called from (representative examples):
  - [transformGroupClauseExpr](../t/transformGroupClauseExpr.md)
  - [transformDistinctClause](../t/transformDistinctClause.md)
  - [transformDistinctOnClause](../t/transformDistinctOnClause.md)

## Notes and Other Information
- Static function internal to parse_clause.c for GROUP BY processing
- More permissive than addTargetToSortList - allows cases where only equality operator exists
- Uses InvalidOid when checking for duplicates with targetIsInSortList
- Sets nulls_first to false by default for grouping operations
- Location parameter is crucial since tle->expr location might point to SELECT item rather than GROUP BY item
- Handles UNKNOWN literal type coercion automatically like addTargetToSortList

## Simplified Source

```c
static List *
addTargetToGroupList(ParseState *pstate, TargetEntry *tle,
                     List *grouplist, List *targetlist, int location)
{
    Oid restype = exprType((Node *) tle->expr);

    // Convert UNKNOWN literals to TEXT type
    if (restype == UNKNOWNOID)
    {
        tle->expr = (Expr *) coerce_type(pstate, (Node *) tle->expr,
                                       restype, TEXTOID, -1,
                                       COERCION_IMPLICIT,
                                       COERCE_IMPLICIT_CAST,
                                       -1);
        restype = TEXTOID;
    }

    // Avoid duplicate entries in group list
    if (!targetIsInSortList(tle, InvalidOid, grouplist))
    {
        SortGroupClause *grpcl = makeNode(SortGroupClause);
        Oid sortop;
        Oid eqop;
        bool hashable;
        ParseCallbackState pcbstate;

        // Set up error reporting context
        setup_parser_errposition_callback(&pcbstate, pstate, location);

        // Get equality operator (required) and sort operator (optional)
        get_sort_group_operators(restype,
                               false, true, false,  // sortable, groupable, less_than
                               &sortop, &eqop, NULL,
                               &hashable);

        cancel_parser_errposition_callback(&pcbstate);

        // Create SortGroupClause for this grouping column
        grpcl->tleSortGroupRef = assignSortGroupRef(tle, targetlist);
        grpcl->eqop = eqop;
        grpcl->sortop = sortop;
        grpcl->nulls_first = false;  // Default for grouping
        grpcl->hashable = hashable;

        grouplist = lappend(grouplist, grpcl);
    }

    return grouplist;
}
```