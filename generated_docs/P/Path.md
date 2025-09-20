# Path

## Location
[src/include/nodes/pathnodes.h:1621-1668](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L1621-L1668)

## Overview
Path is the base structure representing execution paths in PostgreSQL's query planner, containing cost estimates, output specifications, and metadata for different scan and join methods.

## Definition

```c
typedef struct Path
{
	pg_node_attr(no_copy_equal, no_read, no_query_jumble)

	NodeTag		type;

	/* tag identifying scan/join method */
	NodeTag		pathtype;

	/*
	 * the relation this path can build
	 *
	 * We do NOT print the parent, else we'd be in infinite recursion.  We can
	 * print the parent's relids for identification purposes, though.
	 */
	RelOptInfo *parent pg_node_attr(write_only_relids);

	/*
	 * list of Vars/Exprs, cost, width
	 *
	 * We print the pathtarget only if it's not the default one for the rel.
	 */
	PathTarget *pathtarget pg_node_attr(write_only_nondefault_pathtarget);

	/*
	 * parameterization info, or NULL if none
	 *
	 * We do not print the whole of param_info, since it's printed via
	 * RelOptInfo; it's sufficient and less cluttering to print just the
	 * required outer relids.
	 */
	ParamPathInfo *param_info pg_node_attr(write_only_req_outer);

	/* engage parallel-aware logic? */
	bool		parallel_aware;
	/* OK to use as part of parallel plan? */
	bool		parallel_safe;
	/* desired # of workers; 0 = not parallel */
	int			parallel_workers;

	/* estimated size/costs for path (see costsize.c for more info) */
	Cardinality rows;			/* estimated number of result tuples */
	Cost		startup_cost;	/* cost expended before fetching any tuples */
	Cost		total_cost;		/* total cost (assuming all tuples fetched) */

	/* sort ordering of path's output; a List of PathKey nodes; see above */
	List	   *pathkeys;
} Path;
```
## Detailed Description
Path is the fundamental data structure used by PostgreSQL's query planner to represent different ways of executing a query operation. It serves as the base structure for sequential scan paths and other simple plan types, and as the first component for more complex path types that extend it with additional information.

The pathtype field contains the NodeTag of the Plan node that could be built from this Path, providing some redundancy with the Path's own NodeTag but allowing the same Path type to represent multiple Plan types when distinction isn't needed during path processing.

The parent field identifies the relation this Path scans, while pathtarget describes the exact set of output columns the Path would compute. In simple cases, all Paths for a given relation share the same targetlist by having path->pathtarget equal to parent->reltarget.

When param_info is not NULL, it indicates that this path requires parameter values from outer relations during execution, meaning it can only be joined via nestloop joins with this path on the inside. Parameterized paths are also responsible for testing all "movable" join clauses involving this relation and the specified outer relations.

The rows field typically matches parent->rows for simple paths but may be less for parameterized paths and UniquePaths due to filtering by extra join conditions or duplicate removal.

## Parameters / Member Variables
- : NodeTag identifier for the structure type
- : NodeTag identifying the scan/join method and corresponding Plan node type
- : Pointer to the RelOptInfo this path can build (relation information)
- : PathTarget describing the output columns this path computes
- : ParamPathInfo for parameterization details, or NULL if not parameterized
- : Boolean indicating whether to engage parallel-aware logic
- : Boolean indicating if this path is safe to use in parallel plans
- : Desired number of workers for parallel execution (0 = not parallel)
- : Cardinality estimate of result tuples
- : Cost expended before fetching any tuples
- : Total cost assuming all tuples are fetched
- : List of PathKey nodes describing the sort ordering of output rows

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (for type and pathtype identification)
  - RelOptInfo (parent relation information)
  - [PathTarget](PathTarget.md) (output column specifications)
  - [ParamPathInfo](ParamPathInfo.md) (parameterization information)
  - Cardinality (rowcount estimates)
  - Cost (cost estimation values)
  - [List](../L/List.md) (PostgreSQL's list structure for pathkeys)

- Called from (representative examples):
  - [Path](Path.md) is a base structure used throughout the optimizer
  - Extended by specialized path types like IndexPath, NestPath, HashPath, etc.
  - Used in cost estimation functions across src/backend/optimizer/path/costsize.c
  - Referenced in path creation functions in src/backend/optimizer/util/pathnode.c

## Notes and Other Information
- Does not support copying due to circular linkages between RelOptInfo and Path nodes
- Used as-is for sequential scans and simple plan types
- Extended by specialized path types for more complex operations
- The pathkeys field describes sort ordering using PathKey nodes
- Parameterized paths can only participate in nestloop joins as the inner relation
- Cost fields are estimates used by the optimizer to choose between alternative paths
- Parallel execution fields control worker allocation and safety checks