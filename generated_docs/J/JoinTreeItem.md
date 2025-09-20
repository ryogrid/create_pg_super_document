# JoinTreeItem

## Location
[src/backend/optimizer/plan/initsplan.c:59-80](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L59-L80)

## Overview
JoinTreeItem is an internal data structure used during PostgreSQL's query planning process to track information about nodes in the join tree during the deconstruction and qualification distribution phases.

## Definition

```c
typedef struct JoinTreeItem
{
	/* Fields filled during deconstruct_recurse: */
	Node	   *jtnode;			/* jointree node to examine */
	JoinDomain *jdomain;		/* join domain for its ON/WHERE clauses */
	struct JoinTreeItem *jti_parent;	/* JoinTreeItem for this node's
										 * parent, or NULL if it's the top */
	Relids		qualscope;		/* base+OJ Relids syntactically included in
								 * this jointree node */
	Relids		inner_join_rels;	/* base+OJ Relids syntactically included
									 * in inner joins appearing at or below
									 * this jointree node */
	Relids		left_rels;		/* if join node, Relids of the left side */
	Relids		right_rels;		/* if join node, Relids of the right side */
	Relids		nonnullable_rels;	/* if outer join, Relids of the
									 * non-nullable side */
	/* Fields filled during deconstruct_distribute: */
	SpecialJoinInfo *sjinfo;	/* if outer join, its SpecialJoinInfo */
	List	   *oj_joinclauses; /* outer join quals not yet distributed */
	List	   *lateral_clauses;	/* quals postponed from children due to
									 * lateral references */
} JoinTreeItem;
```
## Detailed Description
JoinTreeItem serves as a temporary data structure that facilitates the multi-pass processing of join trees during query planning. The deconstruct_jointree function requires multiple passes because JoinDomains must be fully computed before qualification distribution begins. This structure enables efficient traversal and processing by storing both structural information about the join tree and metadata needed for qualification distribution.

The structure is populated in two main phases: first during deconstruct_recurse (which builds the tree structure and computes relid sets), and then during deconstruct_distribute (which handles qualification distribution). The items are organized in a list following depth-first traversal order, allowing for systematic processing of the entire join tree.

## Parameters / Member Variables
- `*jtnode`: Pointer to the actual jointree node being processed
- `*jdomain`: Associated join domain containing ON/WHERE clause information
- `*jti_parent`: Pointer to the parent JoinTreeItem in the tree hierarchy (NULL for root)
- `qualscope`: Set of base and outer join relation IDs syntactically included in this node
- `inner_join_rels`: Set of relation IDs from inner joins at or below this node
- `left_rels`: For join nodes, the set of relation IDs on the left side
- `right_rels`: For join nodes, the set of relation IDs on the right side
- `nonnullable_rels`: For outer joins, the set of relation IDs from the non-nullable side
- `*sjinfo`: SpecialJoinInfo structure for outer joins (filled during distribution phase)
- `*oj_joinclauses`: List of outer join qualifications awaiting distribution
- `*lateral_clauses`: List of qualifications postponed due to lateral references
## Dependencies
- Functions called/Symbols referenced:
  - JoinDomain
  - [SpecialJoinInfo](../S/SpecialJoinInfo.md)
  - [Node](../N/Node.md)
  - Relids
  - [List](../L/List.md)
- Called from (representative examples):
  - [deconstruct_jointree](../d/deconstruct_jointree.md)
  - [deconstruct_recurse](../d/deconstruct_recurse.md)
  - [deconstruct_distribute](../d/deconstruct_distribute.md)
  - [deconstruct_distribute_oj_quals](../d/deconstruct_distribute_oj_quals.md)
  - [distribute_quals_to_rels](../d/distribute_quals_to_rels.md)

## Notes and Other Information
The JoinTreeItem structures are temporary and can be freed after deconstruct_jointree completes, but their substructures (particularly the relid sets) should not be modified or freed as they may be referenced by RestrictInfo and SpecialJoinInfo nodes. This design pattern allows for efficient memory management while maintaining necessary cross-references during the planning process.

The multi-pass approach enabled by this structure is essential for handling complex queries with outer joins, where the order of processing significantly affects the correctness of the final plan.