# PathTarget

## Location
[src/include/nodes/pathnodes.h:1528-1548](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L1528-L1548)

## Overview
PathTarget represents the targetlist (output columns) that a Path will compute during query planning, containing expression lists, sort/group references, cost information, and metadata about the computed results.

## Definition

```c
typedef struct PathTarget
{
	pg_node_attr(no_copy_equal, no_read, no_query_jumble)

	NodeTag		type;

	/* list of expressions to be computed */
	List	   *exprs;

	/* corresponding sort/group refnos, or 0 */
	Index	   *sortgrouprefs pg_node_attr(array_size(exprs));

	/* cost of evaluating the expressions */
	QualCost	cost;

	/* estimated avg width of result tuples */
	int			width;

	/* indicates if exprs contain any volatile functions */
	VolatileFunctionStatus has_volatile_expr;
} PathTarget;
```
## Detailed Description
PathTarget is a crucial data structure used during query planning to describe what output columns a Path will compute. Each RelOptInfo includes a default PathTarget, which individual Paths may reference directly. However, when a Path computes outputs different from other Paths, a custom PathTarget is created. For example, an index scan might return index expressions that would otherwise need explicit calculation.

The structure contains bare expressions without TargetEntry nodes (though these appear in finished Plans). The sortgrouprefs array corresponds to the exprs list, containing sort/group reference numbers or zero for expressions not referenced by sort/group clauses. This array is often NULL in RelOptInfo.reltarget targets, with upper-level Paths containing this information to handle sort/group operations efficiently.

The structure also tracks cost information for evaluating expressions, estimated tuple width, and whether any expressions contain volatile functions, which affects optimization decisions.

## Parameters / Member Variables
- `type`: NodeTag identifier for the structure type
- `*exprs`: List of expressions to be computed (bare expressions without TargetEntry nodes)
- `pg_node_attr(array_size(exprs))`: Array of sort/group reference numbers corresponding to exprs, or 0 for non-referenced expressions
- `cost`: QualCost structure containing the cost of evaluating the expressions
- `width`: Estimated average width of result tuples in bytes
- `has_volatile_expr`: VolatileFunctionStatus indicating if expressions contain volatile functions
## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (for type identification)
  - [List](../L/List.md) (PostgreSQL's list structure)
  - [QualCost](../Q/QualCost.md) (for cost estimation)
  - Index (for sortgrouprefs array)
  - VolatileFunctionStatus (volatile function indicator)

- Called from (representative examples):
  - [set_pathtarget_cost_width](../s/set_pathtarget_cost_width.md) (src/backend/optimizer/path/costsize.c:6259)
  - standard_qp_extra (src/backend/optimizer/plan/planner.c:151-220)
  - [grouping_planner](../g/grouping_planner.md) (src/backend/optimizer/plan/planner.c:1343-1655)
  - [create_projection_path](../c/create_projection_path.md) (src/backend/optimizer/util/pathnode.c:2688)
  - [make_pathtarget_from_tlist](../m/make_pathtarget_from_tlist.md) (src/backend/optimizer/util/tlist.c:593)

## Notes and Other Information
- Contains bare expressions without TargetEntry nodes for efficiency during planning
- The sortgrouprefs array is often NULL in base relation targets but populated in upper-level operations
- Custom PathTargets are created when Paths compute different outputs than the default
- Used extensively in grouping, windowing, and aggregation planning
- The structure includes node attributes for memory management and debugging purposes
- Width estimation is crucial for memory allocation and cost calculations