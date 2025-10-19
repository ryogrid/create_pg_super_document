# addNode

## Location
[src/backend/access/spgist/spgdoinsert.c:80-111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgdoinsert.c#L80-L111)

## Overview
Creates a new SP-GiST inner tuple containing one additional node compared to the input tuple, with the specified label datum inserted at the given offset position.

## Definition

```c
static SpGistInnerTuple
addNode(SpGistState *state, SpGistInnerTuple tuple, Datum label, int offset)
```
## Detailed Description
This function constructs a new SP-GiST inner tuple that contains all the nodes from the original tuple plus one additional node with the specified label. The new node is inserted at the specified offset position within the node array. The function preserves the original tuple's prefix while expanding the node array. The newly added node initially has an invalid downlink pointer, which will be set later when a target page is determined. This is a fundamental operation used during tree expansion when new branches need to be added to inner nodes.

## Parameters / Member Variables
- `state`: SP-GiST state information containing type-specific configuration and methods
- `tuple`: The original SP-GiST inner tuple to be expanded
- `label`: The label datum for the new node being added
- `offset`: The position where the new node should be inserted (negative values insert at end)

## Dependencies
- Functions called/Symbols referenced:
  - SGITITERATE (macro for iterating through existing nodes)
  - [palloc](../p/palloc.md) (memory allocation)
  - [spgFormNodeTuple](../s/spgFormNodeTuple.md) (creates the new node tuple)
  - [spgFormInnerTuple](../s/spgFormInnerTuple.md) (constructs the final inner tuple)
  - SGITDATUM (extracts datum from tuple)
  - elog (error reporting)
- Called from (representative examples):
  - [spgAddNodeAction](../s/spgAddNodeAction.md)
  - [spgdoinsert](../s/spgdoinsert.md)
  - [spg_text_choose](../s/spg_text_choose.md)
  - [spgist_name_choose](../s/spgist_name_choose.md)

## Notes and Other Information
- The function is static, indicating it's only used within the spgdoinsert.c module
- If offset is negative, the new node is appended at the end of the node array
- The function validates that the offset is within acceptable bounds (0 to nNodes)
- Memory for the nodes array is allocated dynamically based on the expanded size
- The new inner tuple maintains the same prefix as the original but with nNodes + 1 nodes
- Located in src/backend/access/spgist/spgdoinsert.c:80-111

## Simplified Source

```c
static SpGistInnerTuple addNode(SpGistState *state, SpGistInnerTuple tuple,
                               Datum label, int offset) {
    SpGistNodeTuple node, *nodes;
    int i;

    // Handle negative offset (insert at end)
    if (offset < 0) {
        offset = tuple->nNodes;
    } else if (offset > tuple->nNodes) {
        elog(ERROR, "invalid offset for adding node to SPGiST inner tuple");
    }

    // Allocate space for expanded node array
    nodes = palloc(sizeof(SpGistNodeTuple) * (tuple->nNodes + 1));

    // Copy existing nodes, making space for new node at offset
    SGITITERATE(tuple, i, node) {
        if (i < offset) {
            nodes[i] = node;
        } else {
            nodes[i + 1] = node;
        }
    }

    // Create and insert new node at specified offset
    nodes[offset] = spgFormNodeTuple(state, label, false);

    // Form and return new inner tuple with expanded node array
    return spgFormInnerTuple(state,
                           (tuple->prefixSize > 0),
                           SGITDATUM(tuple, state),
                           tuple->nNodes + 1,
                           nodes);
}
```