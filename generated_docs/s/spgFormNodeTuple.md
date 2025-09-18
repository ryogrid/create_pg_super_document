# spgFormNodeTuple

## Location
src/backend/access/spgist/spgutils.c: 952 - 993

## Overview
Constructs an SP-GiST node tuple containing a label value for storage within inner tuples, with the downlink initially set to invalid and filled by the caller later.

## Definition


## Detailed Description
This function creates a node tuple that represents a single node within an SP-GiST inner tuple. Node tuples store label values that help guide tree traversal decisions. The function handles both null and non-null labels appropriately and ensures the resulting tuple fits within PostgreSQL's size constraints.

Key aspects of node tuple formation:

1. **Size calculation**: Uses SGNTHDRSZ for the base header size, plus SpGistGetInnerTypeSize() for non-null label data.

2. **Size validation**: Ensures the total size fits within INDEX_SIZE_MASK constraints to prevent overflow in the t_info field.

3. **Info mask setup**: Sets appropriate flags including INDEX_NULL_MASK for null labels and embeds the size directly in the info mask.

4. **Memory allocation**: Uses palloc0() for zero-initialized memory allocation.

5. **TID initialization**: Sets the tuple identifier to invalid initially - the caller is responsible for setting the actual downlink later.

6. **Label storage**: For non-null labels, uses memcpyInnerDatum() to store the label data following inner tuple conventions.

## Parameters / Member Variables
- : SpGistState structure containing index configuration and type information
- : Datum value representing the node's label (used for tree navigation)
- : Boolean flag indicating whether the label is null

## Dependencies
- Functions called/Symbols referenced:
  - SpGistState (index state structure)
  - SpGistNodeTuple (return type structure)
  - SGNTHDRSZ (node tuple header size constant)
  - SpGistGetInnerTypeSize (calculates label storage size)
  - INDEX_SIZE_MASK (size constraint mask)
  - INDEX_NULL_MASK (null value flag)
  - ItemPointerSetInvalid (TID invalidation function)
  - memcpyInnerDatum (inner datum copying function)
  - SGNTDATAPTR (node tuple data pointer macro)
- Called from (representative examples):
  - addNode (during node addition operations)
  - doPickSplit (during node splitting)
  - spgSplitNodeAction (during split node actions)

## Notes and Other Information
- The downlink (t_tid) is intentionally left invalid and must be filled by the caller
- Node tuples are components of inner tuples in the SP-GiST tree structure
- The size validation prevents index corruption by ensuring tuples fit within PostgreSQL's indexing constraints
- Uses the same storage conventions as other inner tuple components via memcpyInnerDatum()
- The INDEX_VAR_MASK bit is deliberately not set as mentioned in the code comment