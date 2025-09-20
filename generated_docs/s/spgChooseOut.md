# spgChooseOut

## Location
[src/include/access/spgist.h:74-105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/spgist.h#L74-L105)

## Overview
A struct that serves as output parameter for the SP-GiST opclass choose method, containing the decision about how to proceed with tree traversal or modification during insertion.

## Definition

```c
typedef struct spgChooseOut
{
	spgChooseResultType resultType; /* action code, see above */
	union
	{
		struct					/* results for spgMatchNode */
		{
			int			nodeN;	/* descend to this node (index from 0) */
			int			levelAdd;	/* increment level by this much */
			Datum		restDatum;	/* new leaf datum */
		}			matchNode;
		struct					/* results for spgAddNode */
		{
			Datum		nodeLabel;	/* new node's label */
			int			nodeN;	/* where to insert it (index from 0) */
		}			addNode;
		struct					/* results for spgSplitTuple */
		{
			/* Info to form new upper-level inner tuple with one child tuple */
			bool		prefixHasPrefix;	/* tuple should have a prefix? */
			Datum		prefixPrefixDatum;	/* if so, its value */
			int			prefixNNodes;	/* number of nodes */
			Datum	   *prefixNodeLabels;	/* their labels (or NULL for no
											 * labels) */
			int			childNodeN; /* which node gets child tuple */

			/* Info to form new lower-level inner tuple with all old nodes */
			bool		postfixHasPrefix;	/* tuple should have a prefix? */
			Datum		postfixPrefixDatum; /* if so, its value */
		}			splitTuple;
	}			result;
} spgChooseOut;
```
## Detailed Description
spgChooseOut is an output structure used in the SP-GiST (Space-Partitioned Generalized Search Tree) index access method. It is filled by the opclass choose method to indicate how the insertion process should proceed. The structure uses a tagged union design where the resultType field determines which member of the union is valid. The choose method can decide to descend into an existing node, add a new node, or split the current tuple.

## Parameters / Member Variables
- : Enum value indicating which action to take (spgMatchNode, spgAddNode, or spgSplitTuple)
- : Union containing action-specific data:
  - : Index of the child node to descend into (0-based)
  - : Amount to increment the current level by when descending
  - : New datum value to be stored at the leaf level
  - : Label value for the new child node
  - : Position where the new node should be inserted (0-based index)
  - : Whether new upper-level tuple should have a prefix
  - : Prefix value for new upper-level tuple (if applicable)
  - : Number of child nodes in new upper-level tuple
  - : Array of labels for new upper-level tuple's children
  - : Which child node gets the original tuple content
  - : Whether new lower-level tuple should have a prefix  
  - : Prefix value for new lower-level tuple (if applicable)

## Dependencies
- Functions called/Symbols referenced:
  - [spgChooseResultType](spgChooseResultType.md) (enum defining action types)
  - Datum (PostgreSQL generic data value type)
  - [bool](../b/bool.md) (PostgreSQL boolean type)
- Called from (representative examples):
  - [spgSplitNodeAction](spgSplitNodeAction.md) (src/backend/access/spgist/spgdoinsert.c:1717)
  - [spgdoinsert](spgdoinsert.md) (src/backend/access/spgist/spgdoinsert.c:2161)
  - [spg_kd_choose](spg_kd_choose.md) (src/backend/access/spgist/spgkdtreeproc.c:57)
  - [spg_quad_choose](spg_quad_choose.md) (src/backend/access/spgist/spgquadtreeproc.c:118)
  - [spg_text_choose](spg_text_choose.md) (src/backend/access/spgist/spgtextproc.c:187)
  - [spg_box_quad_choose](spg_box_quad_choose.md) (src/backend/utils/adt/geo_spgist.c:420)
  - [inet_spg_choose](../i/inet_spg_choose.md) (src/backend/utils/adt/network_spgist.c:71)

## Notes and Other Information
- This struct is part of the SP-GiST index access method interface
- It works in conjunction with spgChooseIn to allow opclass choose methods to receive input parameters and return decisions
- The union design ensures type safety while supporting multiple different actions
- The spgSplitTuple action is the most complex, allowing the opclass to restructure the tree by creating two levels from one
- Different opclasses may use different subsets of these actions based on their tree organization strategy
- The structure enables efficient tree modification without requiring multiple round trips between the core SP-GiST code and the opclass