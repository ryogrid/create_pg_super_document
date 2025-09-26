# NestLoopParam

## Location
src/include/nodes/plannodes.h: 813 - 820

## Overview
NestLoopParam represents a parameter used in nested loop joins to pass values from outer relation tuples to the inner scan, enabling parameterized nested loop execution.

## Definition

```c
typedef struct NestLoopParam
{
	pg_node_attr(no_equal, no_query_jumble)

	NodeTag		type;
	int			paramno;		/* number of the PARAM_EXEC Param to set */
	Var		   *paramval;		/* outer-relation Var to assign to Param */
} NestLoopParam;
```
## Detailed Description
NestLoopParam is a crucial structure in PostgreSQL's nested loop join implementation that facilitates parameterized execution. When executing a nested loop join, values from the outer relation need to be passed to the inner scan to enable efficient filtering or indexing on the inner side. Each NestLoopParam represents one such parameter passing mechanism.

The structure works by capturing a Var from the outer relation (paramval) and associating it with a PARAM_EXEC parameter number (paramno). During execution, the outer tuple's attribute value is extracted and stored in the corresponding parameter slot, which can then be referenced by the inner scan.

This mechanism is essential for enabling index nested loop joins, where the inner scan can use an index with the parameter value as a key, dramatically improving performance compared to a simple nested loop that scans the entire inner relation for each outer tuple.

## Parameters / Member Variables
- : Standard NodeTag for PostgreSQL node identification
- : The PARAM_EXEC parameter number used to store the outer relation value; this corresponds to an index in the parameter execution array
- : A Var node pointing to a specific attribute in the outer relation whose value will be passed as a parameter to the inner scan

## Dependencies
- Functions called/Symbols referenced:
  - Var (referenced as paramval member type)
  - NodeTag (for type identification)

- Called from (representative examples):
  - ExecNestLoop (executor/nodeNestloop.c:130)
  - set_join_references (optimizer/plan/setrefs.c:2316)
  - replace_nestloop_param_var (optimizer/util/paramassign.c:370)
  - process_subquery_nestloop_params (optimizer/util/paramassign.c:491)

## Notes and Other Information
- The pg_node_attr(no_equal, no_query_jumble) attribute indicates this structure should not participate in equality comparisons or query jumbling operations
- NestLoopParam is stored in a list within the NestLoop plan node (nestParams field)
- The paramval Var must be an OUTER_VAR (varno == OUTER_VAR) referring to an attribute from the outer relation (varattno > 0)
- During execution, parameter values are stored in ParamExecData slots and marked as changed to trigger re-evaluation of the inner plan
- This mechanism enables sophisticated join algorithms like index nested loops and can significantly improve query performance when appropriate indexes exist on the inner relation