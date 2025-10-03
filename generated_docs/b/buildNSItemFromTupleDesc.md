# buildNSItemFromTupleDesc

## Location
[src/backend/parser/parse_relation.c:1294-1353](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L1294-L1353)

## Overview
Builds a ParseNamespaceItem structure from a tuple descriptor, extracting column metadata and creating the namespace representation for parser operations.

## Definition

```c
static ParseNamespaceItem *
buildNSItemFromTupleDesc(RangeTblEntry *rte, Index rtindex,
						 RTEPermissionInfo *perminfo,
						 TupleDesc tupdesc)
```
## Detailed Description
This function constructs a ParseNamespaceItem that encapsulates a relation's column information for use during query parsing. It extracts column metadata from the physical tuple descriptor and builds an array of ParseNamespaceColumn structures containing type information, attribute numbers, and collation details. The function handles dropped columns by leaving their entries as zeroes while maintaining proper indexing alignment. The resulting namespace item includes visibility flags and lateral reference settings with default values.

## Parameters / Member Variables
- `*rte`: The RangeTblEntry for the relation being processed
- `rtindex`: The index position of this RTE in the range table list
- `*perminfo`: Permission information entry for the relation
- `tupdesc`: The tuple descriptor containing physical column information
## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md) (list operations)
  - [palloc0](../p/palloc0.md) (zero-initialized memory allocation)
  - [palloc](../p/palloc.md) (memory allocation)
  - TupleDescAttr (tuple descriptor access macro)
  - [ParseNamespaceItem](../P/ParseNamespaceItem.md) (namespace item structure)
  - [ParseNamespaceColumn](../P/ParseNamespaceColumn.md) (namespace column structure)
- Called from (representative examples):
  - [addRangeTableEntry](../a/addRangeTableEntry.md)
  - [addRangeTableEntryForRelation](../a/addRangeTableEntryForRelation.md)
  - [addRangeTableEntryForFunction](../a/addRangeTableEntryForFunction.md)
  - [addRangeTableEntryForENR](../a/addRangeTableEntryForENR.md)

## Notes and Other Information
- Ensures column name count matches tuple descriptor attribute count via assertion
- Dropped columns are handled by skipping metadata population but maintaining array indexing
- Sets default visibility flags that may be modified later during parsing
- Column attribute numbers are stored as 1-based (varattno + 1) following PostgreSQL conventions
- The namespace item serves as the interface between physical storage and logical query representation
- Both regular and synonym attribute numbers are initialized to the same values initially

## Simplified Source

```c
static ParseNamespaceItem *buildNSItemFromTupleDesc(RangeTblEntry *rte, Index rtindex,
                                                   RTEPermissionInfo *perminfo,
                                                   TupleDesc tupdesc) {
    ParseNamespaceItem *nsitem;
    ParseNamespaceColumn *nscolumns;
    int maxattrs = tupdesc->natts;

    // Verify column name count matches tuple descriptor
    Assert(maxattrs == list_length(rte->eref->colnames));

    // Allocate column metadata array
    nscolumns = (ParseNamespaceColumn *) palloc0(maxattrs * sizeof(ParseNamespaceColumn));

    // Extract column information from tuple descriptor
    for (int varattno = 0; varattno < maxattrs; varattno++) {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, varattno);

        // Skip dropped columns (leave as zeroes)
        if (attr->attisdropped) {
            continue;
        }

        // Set column metadata
        nscolumns[varattno].p_varno = rtindex;
        nscolumns[varattno].p_varattno = varattno + 1;        // 1-based indexing
        nscolumns[varattno].p_vartype = attr->atttypid;
        nscolumns[varattno].p_vartypmod = attr->atttypmod;
        nscolumns[varattno].p_varcollid = attr->attcollation;
        nscolumns[varattno].p_varnosyn = rtindex;
        nscolumns[varattno].p_varattnosyn = varattno + 1;
    }

    // Build and initialize namespace item
    nsitem = (ParseNamespaceItem *) palloc(sizeof(ParseNamespaceItem));
    nsitem->p_names = rte->eref;
    nsitem->p_rte = rte;
    nsitem->p_rtindex = rtindex;
    nsitem->p_perminfo = perminfo;
    nsitem->p_nscolumns = nscolumns;

    // Set default visibility flags
    nsitem->p_rel_visible = true;
    nsitem->p_cols_visible = true;
    nsitem->p_lateral_only = false;
    nsitem->p_lateral_ok = true;

    return nsitem;
}
```