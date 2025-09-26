# nullingrel_info

## Location
[src/backend/optimizer/prep/prepjointree.c:45-54](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L45-L54)

## Overview
The nullingrel_info struct tracks which outer joins potentially null each relation in a query's range table, providing essential information for correct null-handling during query optimization.

## Definition

```c
typedef struct nullingrel_info
{
	/*
	 * For each leaf RTE, nullingrels[rti] is the set of relids of outer joins
	 * that potentially null that RTE.
	 */
	Relids	   *nullingrels;
	/* Length of range table (maximum index in nullingrels[]) */
	int			rtlength;		/* used only for assertion checks */
} nullingrel_info;
```
## Detailed Description
The nullingrel_info structure is a critical component of PostgreSQL's query optimization system that maintains information about outer join nulling relationships. It provides a mapping from each relation in the range table to the set of outer joins that might cause that relation's columns to become NULL in the query result.

This information is essential for correct optimization of queries involving outer joins, as the optimizer needs to understand which relations might be nulled by outer joins when making decisions about predicate pushdown, join reordering, and other optimizations. The structure supports PostgreSQL's sophisticated handling of outer join semantics during the query planning phase.

## Parameters / Member Variables
- `*nullingrels`: Array of Relids pointers, where nullingrels[rti] contains the set of relation IDs of outer joins that potentially null the relation with range table index rti
- `rtlength`: Length of the range table, representing the maximum valid index in the nullingrels array; used primarily for assertion checks to ensure array bounds safety
## Dependencies
- Functions called/Symbols referenced:
  - Relids (PostgreSQL's bitmap set type for relation IDs)
- Called from (representative examples):
  - [pullup_replace_vars_context](../p/pullup_replace_vars_context.md)
  - [pullup_replace_vars_callback](../p/pullup_replace_vars_callback.md)
  - [find_jointree_node_for_rel](../f/find_jointree_node_for_rel.md)
  - [get_nullingrels](../g/get_nullingrels.md)
  - [get_nullingrels_recurse](../g/get_nullingrels_recurse.md)

## Notes and Other Information
This structure is primarily used within the prepjointree.c module during query tree preprocessing. The nullingrels array is indexed by range table index (RTI), making it efficient to look up nulling information for any relation. The rtlength field serves as a safety measure to prevent array bounds violations during debugging builds.