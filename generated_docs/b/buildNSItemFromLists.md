# buildNSItemFromLists

## Location
[src/backend/parser/parse_relation.c:1354-1417](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L1354-L1417)

## Overview
Builds a ParseNamespaceItem structure from lists of column type information, used for non-physical relations like subqueries and CTEs.

## Definition

```c
static ParseNamespaceItem *
buildNSItemFromLists(RangeTblEntry *rte, Index rtindex,
					 List *coltypes, List *coltypmods, List *colcollations)
```
## Detailed Description
This function creates a ParseNamespaceItem for relations that don't have physical tuple descriptors, such as subqueries, table functions, VALUES clauses, and CTEs. It takes separate lists of column types, type modifiers, and collations, and builds the corresponding ParseNamespaceColumn array. Unlike buildNSItemFromTupleDesc, this function doesn't need to handle dropped columns since it works with logical column definitions. The function validates that all input lists have matching lengths and constructs the namespace item with default visibility settings.

## Parameters / Member Variables
- : The RangeTblEntry for the relation being processed
- : The index position of this RTE in the range table list
- : List of column datatype OIDs
- : List of per-column type modifiers
- : List of per-column collation OIDs

## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md) (list operations)
  - [palloc0](../p/palloc0.md) (zero-initialized memory allocation)
  - [palloc](../p/palloc.md) (memory allocation)
  - forthree (parallel iteration macro)
  - lfirst_oid (list element access)
  - lfirst_int (list element access)
  - [ParseNamespaceItem](../P/ParseNamespaceItem.md) (namespace item structure)
  - [ParseNamespaceColumn](../P/ParseNamespaceColumn.md) (namespace column structure)
- Called from (representative examples):
  - [addRangeTableEntryForSubquery](../a/addRangeTableEntryForSubquery.md)
  - [addRangeTableEntryForTableFunc](../a/addRangeTableEntryForTableFunc.md)
  - [addRangeTableEntryForValues](../a/addRangeTableEntryForValues.md)
  - [addRangeTableEntryForCTE](../a/addRangeTableEntryForCTE.md)

## Notes and Other Information
- All input lists must have identical lengths, verified by assertions
- Permission info is set to NULL since these are typically derived relations
- Uses forthree macro for efficient parallel iteration over the three input lists
- Column attribute numbers follow 1-based indexing (varattno + 1)
- Default visibility flags are set but may be modified later during parsing
- Complements buildNSItemFromTupleDesc for handling non-physical relation types
- Both regular and synonym attribute numbers are initialized identically

## Simplified Source

```c
static ParseNamespaceItem *
buildNSItemFromLists(RangeTblEntry *rte, Index rtindex,
                     List *coltypes, List *coltypmods, List *colcollations)
{
    int maxattrs = list_length(coltypes);

    // Validate all lists have same length
    Assert(maxattrs == list_length(rte->eref->colnames));
    Assert(maxattrs == list_length(coltypmods));
    Assert(maxattrs == list_length(colcollations));

    // Allocate column information array
    ParseNamespaceColumn *nscolumns =
        (ParseNamespaceColumn *) palloc0(maxattrs * sizeof(ParseNamespaceColumn));

    // Fill column information from lists
    int varattno = 0;
    forthree(lct, coltypes, lcm, coltypmods, lcc, colcollations)
    {
        nscolumns[varattno].p_varno = rtindex;
        nscolumns[varattno].p_varattno = varattno + 1;
        nscolumns[varattno].p_vartype = lfirst_oid(lct);
        nscolumns[varattno].p_vartypmod = lfirst_int(lcm);
        nscolumns[varattno].p_varcollid = lfirst_oid(lcc);
        nscolumns[varattno].p_varnosyn = rtindex;
        nscolumns[varattno].p_varattnosyn = varattno + 1;
        varattno++;
    }

    // Build namespace item
    ParseNamespaceItem *nsitem = (ParseNamespaceItem *) palloc(sizeof(ParseNamespaceItem));
    nsitem->p_names = rte->eref;
    nsitem->p_rte = rte;
    nsitem->p_rtindex = rtindex;
    nsitem->p_perminfo = NULL;
    nsitem->p_nscolumns = nscolumns;

    // Set default visibility flags
    nsitem->p_rel_visible = true;
    nsitem->p_cols_visible = true;
    nsitem->p_lateral_only = false;
    nsitem->p_lateral_ok = true;

    return nsitem;
}
```