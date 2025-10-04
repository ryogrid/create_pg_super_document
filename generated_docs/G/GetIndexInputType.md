# GetIndexInputType

## Location
[src/backend/access/spgist/spgutils.c:115-159](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgutils.c#L115-L159)

## Overview
GetIndexInputType determines the nominal input data type for an index column, preferring the opclass's opcintype or falling back to the base type of the heap column or expression.

## Definition

```c
static Oid
GetIndexInputType(Relation index, AttrNumber indexcol)
```
## Detailed Description
This function determines the appropriate input data type for a given index column by implementing a preference hierarchy. It first checks the opclass's opcintype, and if that's a polymorphic type, it examines the actual input type from either a simple heap column or an index expression. The function ensures that non-polymorphic opclasses don't receive information about binary-compatible types (e.g., preferring "text" over "varchar"), and it flattens domain types when consulting actual input types.

The function handles both simple index columns (referencing heap table columns) and expression-based index columns. For expression columns, it walks through the cached index expressions to find the appropriate expression and determine its type.

## Parameters / Member Variables
- `index`: The relation representing the index
- `indexcol`: The column number in the index (1-based) to get the input type for
## Dependencies
- Functions called/Symbols referenced:
  - IsPolymorphicType (check if type is polymorphic)
  - [get_atttype](../g/get_atttype.md) (get attribute type from relation)
  - [getBaseType](../g/getBaseType.md) (flatten domain types to base types)
  - [RelationGetIndexExpressions](../R/RelationGetIndexExpressions.md) (get index expressions if not cached)
  - [list_head](../l/list_head.md) (get first element of list)
  - [lnext](../l/lnext.md) (get next element in list)
  - [exprType](../e/exprType.md) (get type of expression node)
- Called from (representative examples):
  - [spgGetCache](../s/spgGetCache.md) (at src/backend/access/spgist/spgutils.c:204)

## Notes and Other Information
- Located in src/backend/access/spgist/spgutils.c:115-159
- This is a static function, only used within the spgutils.c file
- The function prioritizes opclass opcintype over actual input types to maintain compatibility
- Handles domain type flattening to provide base types to opclasses
- Includes optimization to avoid copying index expressions when they're already cached
- Contains error checking for mismatched expression counts
- The comment suggests this function might be moved elsewhere if other index access methods need similar functionality

## Simplified Source

```c
static Oid GetIndexInputType(Relation index, AttrNumber indexcol) {
    Oid opcintype;
    AttrNumber heapcol;
    List *indexprs;
    ListCell *indexpr_item;

    // Get opclass input type
    opcintype = index->rd_opcintype[indexcol - 1];
    if (!IsPolymorphicType(opcintype))
        return opcintype; // Use non-polymorphic opclass type

    // For polymorphic types, determine actual input type
    heapcol = index->rd_index->indkey.values[indexcol - 1];
    if (heapcol != 0) {
        // Simple column reference
        return getBaseType(get_atttype(index->rd_index->indrelid, heapcol));
    }

    // Expression column - find the corresponding expression
    if (index->rd_indexprs)
        indexprs = index->rd_indexprs;
    else
        indexprs = RelationGetIndexExpressions(index);

    indexpr_item = list_head(indexprs);
    for (int i = 1; i <= index->rd_index->indnkeyatts; i++) {
        if (index->rd_index->indkey.values[i - 1] == 0) {
            // Expression column
            if (indexpr_item == NULL)
                elog(ERROR, "wrong number of index expressions");
            if (i == indexcol)
                return getBaseType(exprType((Node *) lfirst(indexpr_item)));
            indexpr_item = lnext(indexprs, indexpr_item);
        }
    }

    elog(ERROR, "wrong number of index expressions");
    return InvalidOid;
}
```