# spgPickSplitOut

## Location
[src/include/access/spgist.h:117-127](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/spgist.h#L117-L127)

## Overview
spgPickSplitOut is a structure that defines the output parameters for SP-GiST (Space-Partitioned Generalized Search Tree) index pick split operations, used to specify how leaf tuples should be redistributed when splitting an inner node.

## Definition


## Detailed Description
The spgPickSplitOut structure is used as an output parameter in SP-GiST index pick split operations. When an inner node becomes full and needs to be split, the pick split function populates this structure to specify how the existing leaf tuples should be redistributed among the new child nodes. This structure provides complete information about the new inner tuple configuration, including whether it should have a prefix, how many child nodes it should have, what labels those nodes should get, and how existing leaf tuples should be mapped to the new nodes.

## Parameters / Member Variables
- : Boolean flag indicating whether the new inner tuple should have a prefix value
- : The prefix value to be stored in the new inner tuple (only valid if hasPrefix is true)
- : The number of child nodes that the new inner tuple should have
- : Array of Datum values representing labels for each child node (can be NULL if nodes don't need labels)
- : Array mapping each existing leaf tuple to a child node index (indices correspond to positions in nodeLabels array)
- : Array of Datum values to be stored in the new leaf tuples after redistribution

## Dependencies
- Functions called/Symbols referenced:
  - Datum (PostgreSQL data type)
  - [bool](../b/bool.md) (standard boolean type)
- Called from (representative examples):
  - [checkAllTheSame](../c/checkAllTheSame.md) (src/backend/access/spgist/spgdoinsert.c:599)
  - [doPickSplit](../d/doPickSplit.md) (src/backend/access/spgist/spgdoinsert.c:684)
  - [spg_kd_picksplit](spg_kd_picksplit.md) (src/backend/access/spgist/spgkdtreeproc.c:111)
  - [spg_quad_picksplit](spg_quad_picksplit.md) (src/backend/access/spgist/spgquadtreeproc.c:172)
  - [spg_text_picksplit](spg_text_picksplit.md) (src/backend/access/spgist/spgtextproc.c:336)
  - [spg_box_quad_picksplit](spg_box_quad_picksplit.md) (src/backend/utils/adt/geo_spgist.c:444)
  - [inet_spg_picksplit](../i/inet_spg_picksplit.md) (src/backend/utils/adt/network_spgist.c:168)
  - [spg_range_quad_picksplit](spg_range_quad_picksplit.md) (src/backend/utils/adt/rangetypes_spgist.c:203)

## Notes and Other Information
- This structure is primarily used by SP-GiST operator class implementations to communicate split decisions back to the SP-GiST access method
- The mapTuplesToNodes and leafTupleDatums arrays must have the same length as the number of leaf tuples being redistributed
- Memory management for the arrays (nodeLabels, mapTuplesToNodes, leafTupleDatums) is typically handled by the caller
- Different data types (geometric, text, network addresses, ranges) implement their own pick split logic using this common output structure