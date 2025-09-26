# SpGistState

## Location
[src/include/access/spgist_private.h:144-162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/spgist_private.h#L144-L162)

## Overview
SpGistState is the central state structure for SP-GiST index operations, containing all necessary information for both insertion and search operations within a SP-GiST index.

## Definition

```c
typedef struct SpGistState
{
	Relation	index;			/* index we're working with */

	spgConfigOut config;		/* filled in by opclass config method */

	SpGistTypeDesc attType;		/* type of values to be indexed/restored */
	SpGistTypeDesc attLeafType; /* type of leaf-tuple values */
	SpGistTypeDesc attPrefixType;	/* type of inner-tuple prefix values */
	SpGistTypeDesc attLabelType;	/* type of node label values */

	/* leafTupDesc typically points to index's tupdesc, but not always */
	TupleDesc	leafTupDesc;	/* descriptor for leaf-level tuples */

	char	   *deadTupleStorage;	/* workspace for spgFormDeadTuple */

	TransactionId redirectXid;	/* XID to use when creating a redirect tuple */
	bool		isBuild;		/* true if doing index build */
} SpGistState;
```
## Detailed Description
SpGistState serves as the comprehensive operational context for all SP-GiST index activities. It consolidates essential information including the index relation, operator class configuration, and type descriptors for different kinds of values handled by the index. This structure enables SP-GiST to efficiently manage the complex type relationships inherent in space-partitioned indexes where different node levels may handle different data representations.

The structure is designed to support SP-GiST's sophisticated approach to space partitioning, where inner nodes may store compressed or transformed representations of the original data (prefixes, labels) while leaf nodes contain the actual indexed values or suffixes. The type descriptors allow proper handling of these varied data representations throughout the index hierarchy.

## Parameters / Member Variables
- : The Relation structure representing the SP-GiST index being operated on
- : Configuration output from the operator class, containing function pointers and parameters
- : Type descriptor for the original values being indexed or restored
- : Type descriptor for values stored in leaf tuples (may differ from attType)
- : Type descriptor for prefix values stored in inner tuples
- : Type descriptor for node label values used in tree navigation
- : Tuple descriptor for leaf-level tuples (usually points to index's tupdesc)
- : Workspace buffer for constructing dead tuples during operations
- : Transaction ID to assign when creating redirect tuples during splits
- : Boolean flag indicating whether currently performing index build operations

## Dependencies
- Functions called/Symbols referenced:
  - [spgConfigOut](../s/spgConfigOut.md) (operator class configuration)
  - [SpGistTypeDesc](SpGistTypeDesc.md) (type descriptors)
  - [Relation](../R/Relation.md) (index relation)
  - [TupleDesc](../T/TupleDesc.md) (tuple descriptors)
  - TransactionId (transaction management)

- Called from (representative examples):
  - [initSpGistState](../i/initSpGistState.md) (spgutils.c:340)
  - [spgdoinsert](../s/spgdoinsert.md) (spgdoinsert.c:1914)
  - [spginsert](../s/spginsert.md) (spginsert.c:189)
  - [spgFormLeafTuple](../s/spgFormLeafTuple.md) (spgutils.c:863)
  - [doPickSplit](../d/doPickSplit.md) (spgdoinsert.c:677)

## Notes and Other Information
- Central to all SP-GiST operations, used in both insertion and search code paths
- Maintains type information for multiple data representations within a single index
- Supports SP-GiST's ability to store different data types at different tree levels
- Essential for proper transaction handling during index modifications
- The deadTupleStorage provides efficient workspace management for tuple operations
- Used extensively throughout spgdoinsert.c, spgutils.c, and spgvacuum.c
- Critical for maintaining consistency during complex operations like node splits