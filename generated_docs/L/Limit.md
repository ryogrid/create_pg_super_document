# Limit

## Location
[src/include/nodes/plannodes.h:1270-1294](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L1270-L1294)

## Overview
Limit is a plan node structure that implements LIMIT and OFFSET clauses in PostgreSQL queries, controlling the number of rows returned from the execution pipeline.

## Definition
```c
typedef struct Limit
{
    Plan        plan;

    /* OFFSET parameter, or NULL if none */
    Node       *limitOffset;

    /* COUNT parameter, or NULL if none */
    Node       *limitCount;

    /* limit type */
    LimitOption limitOption;

    /* number of columns to check for similarity  */
    int         uniqNumCols;

    /* their indexes in the target list */
    AttrNumber *uniqColIdx pg_node_attr(array_size(uniqNumCols));

    /* equality operators to compare with */
    Oid        *uniqOperators pg_node_attr(array_size(uniqNumCols));

    /* collations for equality comparisons */
    Oid        *uniqCollations pg_node_attr(array_size(uniqNumCols));
} Limit;
```

## Detailed Description
The Limit plan node implements row count limitation functionality for SQL LIMIT and OFFSET clauses. It processes tuples from its child plan and applies the specified limiting logic. Since PostgreSQL 8.2, the offset and count expressions are expected to yield int8 values rather than int4 for consistency with SQL standards.

The node supports both standard LIMIT functionality and more complex variants like FETCH FIRST...WITH TIES. For WITH TIES operations, it uses the uniq* fields to compare rows for equality to determine which additional 'tied' rows should be included beyond the basic limit count.

The node can handle dynamic limit and offset values through expression evaluation, making it flexible for prepared statements and complex query scenarios.

## Parameters / Member Variables
- `plan`: Base Plan structure containing common plan node information like cost estimates, target lists, and child plan references
- `limitOffset`: Node representing the OFFSET expression, or NULL if no offset is specified
- `limitCount`: Node representing the COUNT/LIMIT expression, or NULL if no limit is specified
- `limitOption`: Specifies the type of limit operation (LIMIT_OPTION_COUNT for standard LIMIT, LIMIT_OPTION_WITH_TIES for FETCH FIRST...WITH TIES)
- `uniqNumCols`: Number of columns to check for equality when determining tied rows (used with WITH TIES)
- `uniqColIdx`: Array containing the target list indexes of columns used for tie comparison
- `uniqOperators`: Array of equality operator OIDs for comparing the tie columns
- `uniqCollations`: Array of collation OIDs for string comparison in tie detection

## Dependencies
- Functions called/Symbols referenced:
  - [Plan](../P/Plan.md) (base structure)
  - [Node](../N/Node.md) (expression tree nodes)
  - [LimitOption](LimitOption.md) (enumeration for limit types)
  - AttrNumber (column attribute numbers)
  - Oid (object identifiers)
- Called from (representative examples):
  - [ExecInitLimit](../E/ExecInitLimit.md) (executor initialization)
  - [create_limit_plan](../c/create_limit_plan.md) (planner)
  - [make_limit](../m/make_limit.md) (plan creation utility)
  - [ExecInitNode](../E/ExecInitNode.md) (generic executor initialization)

## Notes and Other Information
- Since PostgreSQL 8.2, limit and offset expressions yield int8 (bigint) values instead of int4 (integer) for better range support
- The WITH TIES functionality requires sorting to be meaningful, so Limit nodes with LIMIT_OPTION_WITH_TIES typically appear above Sort nodes
- Dynamic limit/offset evaluation allows for prepared statements where the limit values are parameters
- The uniq* arrays are only populated and used when limitOption is LIMIT_OPTION_WITH_TIES
- Proper collation handling ensures correct string comparison semantics across different locales when determining tied rows