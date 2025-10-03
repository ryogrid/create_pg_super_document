# doPickSplit

## Location
[src/backend/access/spgist/spgdoinsert.c:677-1458](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgdoinsert.c#L677-L1458)

## Overview
Splits a leaf tuple chain when there's insufficient space to add a new leaf tuple, redistributing tuples across current and new pages according to picksplit algorithm rules.

## Definition

```c
static bool
doPickSplit(Relation index, SpGistState *state,
			SPPageDesc *current, SPPageDesc *parent,
			SpGistLeafTuple newLeafTuple,
			int level, bool isNulls, bool isNew)
```
## Detailed Description
The  function handles the complex process of splitting leaf tuple chains in SP-GiST when a page lacks sufficient space for a new tuple. It creates one or more new chains distributed across the current page and an additional leaf page, while creating a new inner tuple to organize the split result. The function uses the opclass-defined picksplit method to determine how to redistribute tuples, potentially stripping prefixes to make tuples smaller. The split ensures at least two chains are created, guaranteeing forward progress even with unbalanced splits.

## Parameters / Member Variables
- `index`: The SP-GiST index relation being modified
- `*state`: SP-GiST state information containing opclass details and configuration
- `*current`: Page descriptor for the current page containing the leaf tuple chain
- `*parent`: Page descriptor for the parent page (containing the downlink to current)
- `newLeafTuple`: The new leaf tuple that triggered the need for splitting
- `level`: Current tree level for prefix stripping decisions
- `isNulls`: Whether this operation is on the nulls tree
- `isNew`: Whether the current page is newly created
## Dependencies
- Functions called/Symbols referenced:
  - [checkAllTheSame](../c/checkAllTheSame.md)
  - [spgFormLeafTuple](../s/spgFormLeafTuple.md)  
  - [spgFormInnerTuple](../s/spgFormInnerTuple.md)
  - [spgFormNodeTuple](../s/spgFormNodeTuple.md)
  - [SpGistGetBuffer](../S/SpGistGetBuffer.md)
  - [SpGistPageAddNewItem](../S/SpGistPageAddNewItem.md)
  - [saveNodeLink](../s/saveNodeLink.md)
  - [setRedirectionTuple](../s/setRedirectionTuple.md)
- Called from (representative examples):
  - [spgdoinsert](../s/spgdoinsert.md)

## Notes and Other Information
Returns true if the new leaf tuple was successfully inserted during the split operation, false if the caller needs to retry (typically due to space constraints or unbalanced splits). The function handles WAL logging for crash recovery and manages buffer locking to prevent deadlocks. Special handling is required for root page splits, which transform a leaf page into an inner page. The algorithm may require multiple iterations if the picksplit result is highly unbalanced or if prefix stripping is insufficient to make tuples fit.