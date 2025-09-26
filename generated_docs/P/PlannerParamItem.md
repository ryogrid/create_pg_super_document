# PlannerParamItem

## Location
[src/include/nodes/pathnodes.h:3185-3193](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L3185-L3193)

## Overview
PlannerParamItem represents a PARAM_EXEC slot assignment that enables passing values between different parts of a query plan, including from outer to inner subqueries, from subqueries back to parent queries, and between NestLoop plan nodes.

## Definition
```c
typedef struct PlannerParamItem
{
    pg_node_attr(no_copy_equal, no_read, no_query_jumble)

    NodeTag     type;

    Node       *item;        /* the Var, PlaceHolderVar, or Aggref */
    int         paramId;     /* its assigned PARAM_EXEC slot number */
} PlannerParamItem;
```

## Detailed Description
PlannerParamItem is a fundamental component of PostgreSQL's parameter passing mechanism for complex queries involving subqueries and nested loop joins. At runtime, PARAM_EXEC slots serve as communication channels that allow values to flow between different plan nodes in various directions:

1. **Downward flow**: Passing values from outer queries into subqueries to handle outer references
2. **Upward flow**: Returning results from subplans to their parent queries  
3. **Lateral flow**: Passing values from the outer relation of a NestLoop to parameterize its inner scan

The planner maintains these items in root->plan_params during query planning. Each parent query level's plan_params contains values required by the current subquery being planned. During create_plan(), the same mechanism tracks values that must pass from outer to inner sides of NestLoop plan nodes.

The system supports three types of items:
- **Var nodes**: Variables from the current level that need to be passed to subqueries or NestLoop inner scans
- **PlaceHolderVar nodes**: Placeholder expressions with contained sub-expressions, similar to Vars but with more complex evaluation semantics
- **Aggref nodes**: Aggregate expressions that serve as outer references for subqueries

The planner performs duplicate elimination for Var and PlaceHolderVar parameters within the same scope (either parameters passed to a single subquery or nestloop parameters within a single query level), but doesn't optimize Aggref duplicates.

## Parameters / Member Variables
- `type`: Standard NodeTag for node type identification
- `*item`: Pointer to the actual expression node being parameterized - can be a Var, PlaceHolderVar, or Aggref representing the value that needs to be passed through the PARAM_EXEC mechanism
- `paramId`: The unique PARAM_EXEC slot number assigned to this parameter by the planner, ensuring non-conflicting parameter passing throughout the query plan
## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (node type system)
  - [Node](../N/Node.md) (base node type for expressions)

- Called from (representative examples):
  - [assign_param_for_var](../a/assign_param_for_var.md) (in paramassign.c:69, 79, 103)
  - [assign_param_for_placeholdervar](../a/assign_param_for_placeholdervar.md) (in paramassign.c:152, 162, 178)
  - [replace_outer_agg](../r/replace_outer_agg.md) (in paramassign.c:227, 244)
  - [replace_outer_grouping](../r/replace_outer_grouping.md) (in paramassign.c:273, 291)
  - [replace_outer_merge_support](../r/replace_outer_merge_support.md) (in paramassign.c:320, 342)
  - [SS_identify_outer_params](../S/SS_identify_outer_params.md) (in subselect.c:2097)
  - [build_subplan](../b/build_subplan.md) (in subselect.c:353)

## Notes and Other Information
- Uses pg_node_attr with no_copy_equal, no_read, no_query_jumble attributes to control node processing behavior
- Critical for enabling complex query patterns like correlated subqueries, EXISTS clauses, and parameterized nested loop joins
- The duplicate elimination optimization helps reduce memory usage and execution overhead by reusing parameter slots for identical expressions
- Parameter IDs are managed globally to ensure no conflicts occur when the same slot might be used for different purposes
- Part of PostgreSQL's sophisticated query execution framework that enables efficient handling of complex multi-level queries
- Separate from setParam items for subplans, which are tracked differently via root->glob->paramExecTypes rather than PlannerParamItems