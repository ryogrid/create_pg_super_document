# build_index_tlist

## Location
[src/backend/optimizer/util/plancat.c:1885-1946](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/plancat.c#L1885-L1946)

## Overview
Constructs a target list representing the columns of a specified index, with each column represented by a Var or expression in base-relation terms.

## Definition

```c
static List *
build_index_tlist(PlannerInfo *root, IndexOptInfo *index,
				  Relation heapRelation)
```
## Detailed Description
This function builds a target list that represents the structure of an index by creating appropriate expressions for each indexed column. Unlike , this function does not need to handle dropped columns since indexes never contain dropped columns.

The function processes two types of index columns:
1. **Simple columns** (indexkey != 0): Regular table columns referenced by their attribute number, including system columns (negative indexkey values)
2. **Expression columns** (indexkey == 0): Complex expressions stored in the index's  list

For simple columns, it creates Var nodes using the column's type information from either the heap relation's tuple descriptor or system attribute definitions. For expression columns, it retrieves the pre-parsed expressions from the IndexOptInfo structure.

The function ensures consistency by validating that the number of expression columns matches the expected count based on zero indexkey values.

## Parameters / Member Variables
- `*root`: PlannerInfo containing global planner state (currently unused in implementation)
- `*index`: IndexOptInfo structure containing index metadata including column keys and expressions
- `heapRelation`: Relation structure for the base table to access attribute information
## Dependencies
- Functions called/Symbols referenced:
  - [list_head](../l/list_head.md) (gets first element of index expressions list)
  - [lnext](../l/lnext.md) (advances to next list element)
  - [SystemAttributeDefinition](../S/SystemAttributeDefinition.md) (gets system attribute information for negative indexkeys)
  - TupleDescAttr (accesses heap relation attribute information)
  - [makeVar](../m/makeVar.md) (creates Var nodes for simple columns)
  - [makeTargetEntry](../m/makeTargetEntry.md) (creates target list entries)
  - [IndexOptInfo](../I/IndexOptInfo.md) (index optimization information structure)

- Called from (representative examples):
  - [get_relation_info](../g/get_relation_info.md) (src/backend/optimizer/util/plancat.c:455)

## Notes and Other Information
- Static function (internal to plancat.c module)
- No failure cases needed since indexes cannot contain dropped columns
- Handles both regular columns and expression-based index columns
- System columns (negative indexkey) are supported via SystemAttributeDefinition
- Expression columns are taken from the pre-parsed  list in IndexOptInfo
- Critical for index-only scans and index scan planning
- Target entries use 1-based numbering (i + 1) to match PostgreSQL conventions
- Location: src/backend/optimizer/util/plancat.c:1885-1946

## Simplified Source

```c
static List *
build_index_tlist(PlannerInfo *root, IndexOptInfo *index, Relation heapRelation)
{
    List *tlist = NIL;
    Index varno = index->rel->relid;
    ListCell *indexpr_item;
    int i;

    // Start with first expression in the index expressions list
    indexpr_item = list_head(index->indexprs);

    // Process each index column
    for (i = 0; i < index->ncolumns; i++) {
        int indexkey = index->indexkeys[i];
        Expr *indexvar;

        if (indexkey != 0) {
            // Simple column reference
            const FormData_pg_attribute *att_tup;

            // Get attribute info (system or regular column)
            if (indexkey < 0)
                att_tup = SystemAttributeDefinition(indexkey);
            else
                att_tup = TupleDescAttr(heapRelation->rd_att, indexkey - 1);

            // Create Var node for the column
            indexvar = (Expr *) makeVar(varno, indexkey,
                                        att_tup->atttypid,
                                        att_tup->atttypmod,
                                        att_tup->attcollation, 0);
        } else {
            // Expression column - get from indexprs list
            if (indexpr_item == NULL)
                elog(ERROR, "wrong number of index expressions");

            indexvar = (Expr *) lfirst(indexpr_item);
            indexpr_item = lnext(index->indexprs, indexpr_item);
        }

        // Add to target list with 1-based position
        tlist = lappend(tlist, makeTargetEntry(indexvar, i + 1, NULL, false));
    }

    // Verify we used all expressions
    if (indexpr_item != NULL)
        elog(ERROR, "wrong number of index expressions");

    return tlist;
}
```