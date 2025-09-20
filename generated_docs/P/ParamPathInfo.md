# ParamPathInfo

## Location
[src/include/nodes/pathnodes.h:1575-1585](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L1575-L1585)

## Overview
ParamPathInfo stores common information for all parameterized paths of a given relation with specific required outer relations, including estimated rowcount and clause information for parameterization.

## Definition

```c
typedef struct ParamPathInfo
{
	pg_node_attr(no_copy_equal, no_read, no_query_jumble)

	NodeTag		type;

	Relids		ppi_req_outer;	/* rels supplying parameters used by path */
	Cardinality ppi_rows;		/* estimated number of result tuples */
	List	   *ppi_clauses;	/* join clauses available from outer rels */
	Bitmapset  *ppi_serials;	/* set of rinfo_serial for enforced quals */
} ParamPathInfo;
```
## Detailed Description
ParamPathInfo is a shared data structure that links to all parameterized paths for a given relation with the same set of required outer relations. The primary purpose is to store common information, particularly the estimated rowcount for this specific parameterization, to avoid recalculations and ensure consistency across all paths using the same parameterization.

The structure serves different roles depending on the context. For base relation paths, ppi_clauses contains the relevant join clauses, and ppi_serials tracks which quals are enforced by the path. However, for join cases, ppi_clauses is NIL because the relevant clause set varies based on how the join is formed - these clauses appear instead in each parameterized join path's joinrestrictinfo list. Append relations also don't populate ppi_clauses.

The ppi_serials field contains rinfo_serial numbers for quals enforced by the path and is only maintained for base relations. While this information could be constructed on-the-fly from ppi_clauses, PostgreSQL materializes a copy for efficiency.

## Parameters / Member Variables
- : NodeTag identifier for the structure type
- : Relids bitmap identifying the outer relations that supply parameters used by this path
- : Cardinality estimate of the number of result tuples for this parameterization
- : List of join clauses available from outer relations (only used for base relations)
- : Bitmapset containing rinfo_serial numbers for quals enforced by this path (base relations only)

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (for type identification)
  - Relids (bitmap of relation IDs)
  - Cardinality (rowcount estimate type)
  - [List](../L/List.md) (PostgreSQL's list structure)
  - [Bitmapset](../B/Bitmapset.md) (bitmap data structure)

- Called from (representative examples):
  - [cost_seqscan](../c/cost_seqscan.md) (src/backend/optimizer/path/costsize.c:285)
  - [cost_bitmap_heap_scan](../c/cost_bitmap_heap_scan.md) (src/backend/optimizer/path/costsize.c:1014)
  - [get_restriction_qual_cost](../g/get_restriction_qual_cost.md) (src/backend/optimizer/path/costsize.c:4966)
  - get_baserel_parampathinfo (src/backend/optimizer/util/relnode.c:1560-1670)
  - get_joinrel_parampathinfo (src/backend/optimizer/util/relnode.c:1678-1867)

## Notes and Other Information
- Ensures consistent rowcount estimates across all paths with the same parameterization
- ppi_clauses is only meaningful for base relation paths, not joins or append relations
- ppi_serials is maintained only for base relations to track enforced quals
- Used extensively in cost estimation functions throughout the optimizer
- Helps avoid redundant calculations during path generation
- The structure includes node attributes for memory management and debugging