# spgChooseIn

## Location
[src/include/access/spgist.h:53-65](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/spgist.h#L53-L65)

## Overview
A struct that serves as input parameter for the SP-GiST opclass choose method, containing information about the datum to be indexed and the current state of the inner tuple being traversed.

## Definition

```c
typedef struct spgChooseIn
{
	Datum		datum;			/* original datum to be indexed */
	Datum		leafDatum;		/* current datum to be stored at leaf */
	int			level;			/* current level (counting from zero) */

	/* Data from current inner tuple */
	bool		allTheSame;		/* tuple is marked all-the-same? */
	bool		hasPrefix;		/* tuple has a prefix? */
	Datum		prefixDatum;	/* if so, the prefix value */
	int			nNodes;			/* number of nodes in the inner tuple */
	Datum	   *nodeLabels;		/* node label values (NULL if none) */
} spgChooseIn;
```
## Detailed Description
spgChooseIn is an input structure used in the SP-GiST (Space-Partitioned Generalized Search Tree) index access method. It is passed to the opclass choose method during insertion operations to help the opclass decide which branch of the tree to follow or how to modify the tree structure. The structure contains both the data being inserted and information about the current inner tuple being examined.

## Parameters / Member Variables
- : The original datum value that is being indexed. This is the value as provided by the user
- : The current datum value that will be stored at the leaf level. This may differ from the original datum due to opclass-specific transformations
- : The current depth level in the tree, starting from zero at the root level
- : Boolean flag indicating whether the current inner tuple is marked as all-the-same, meaning all its subtrees contain equivalent values
- : Boolean flag indicating whether the current inner tuple has an associated prefix value
- : The prefix value associated with the current inner tuple, if hasPrefix is true
- : The number of child nodes in the current inner tuple
- : Array of label values for each child node, or NULL if the inner tuple doesn't use labels

## Dependencies
- Functions called/Symbols referenced:
  - Datum (PostgreSQL generic data value type)
  - [bool](../b/bool.md) (PostgreSQL boolean type)
- Called from (representative examples):
  - [spgdoinsert](spgdoinsert.md) (src/backend/access/spgist/spgdoinsert.c:2160)
  - [spg_kd_choose](spg_kd_choose.md) (src/backend/access/spgist/spgkdtreeproc.c:56)
  - [spg_quad_choose](spg_quad_choose.md) (src/backend/access/spgist/spgquadtreeproc.c:117)
  - [spg_text_choose](spg_text_choose.md) (src/backend/access/spgist/spgtextproc.c:186)
  - [spg_box_quad_choose](spg_box_quad_choose.md) (src/backend/utils/adt/geo_spgist.c:419)
  - [inet_spg_choose](../i/inet_spg_choose.md) (src/backend/utils/adt/network_spgist.c:70)
  - [spg_range_quad_choose](spg_range_quad_choose.md) (src/backend/utils/adt/rangetypes_spgist.c:133)

## Notes and Other Information
- This struct is part of the SP-GiST index access method interface
- It works in conjunction with spgChooseOut to allow opclass choose methods to receive input parameters and return decisions about tree traversal
- The choose method is called during insertion to determine which path through the tree to take
- Different opclasses use different aspects of this structure depending on their tree organization strategy
- The level information helps opclasses make decisions that may vary by depth in the tree
- The allTheSame flag is used for optimization when all values in a subtree are equivalent according to the opclass