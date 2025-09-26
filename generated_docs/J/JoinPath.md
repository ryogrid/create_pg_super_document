# JoinPath

## Location
[src/include/nodes/pathnodes.h:2065-2086](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L2065-L2086)

## Overview
JoinPath is an abstract base structure for all join-type paths in PostgreSQL query planning, containing common fields shared by all join algorithms.

## Definition
```c
typedef struct JoinPath
{
    pg_node_attr(abstract)

    Path        path;

    JoinType    jointype;

    bool        inner_unique;    /* each outer tuple provably matches no more
                                 * than one inner tuple */

    Path       *outerjoinpath;   /* path for the outer side of the join */
    Path       *innerjoinpath;   /* path for the inner side of the join */

    List       *joinrestrictinfo; /* RestrictInfos to apply to join */

    /*
     * See the notes for RelOptInfo and ParamPathInfo to understand why
     * joinrestrictinfo is needed in JoinPath, and cant be merged into the
     * parent RelOptInfo.
     */
} JoinPath;
```

## Detailed Description
JoinPath serves as the abstract base structure for all join operation paths in PostgreSQL query planning system. It encapsulates the common information needed by all join algorithms including nested loop joins, merge joins, and hash joins. The structure is marked as abstract (pg_node_attr(abstract)) indicating it is never instantiated directly but always as part of a concrete join path subtype.

The JoinPath contains essential join-specific information such as the join type (INNER, LEFT, RIGHT, FULL, etc.), paths for both the outer and inner sides of the join, and the join restrictions that need to be applied. The inner_unique flag is a crucial optimization hint that indicates whether each outer tuple is guaranteed to match at most one inner tuple, which can significantly affect join algorithm choice and cost estimation.

## Parameters / Member Variables
- `path`: Base Path structure containing standard path information (cost, cardinality, ordering, etc.)
- `jointype`: The type of join operation (JoinType enum: INNER, LEFT, RIGHT, FULL, SEMI, ANTI, etc.)
- `inner_unique`: Boolean flag indicating whether each outer tuple matches at most one inner tuple (enables important optimizations)
- `outerjoinpath`: Pointer to the Path structure for the outer (left) side of the join
- `innerjoinpath`: Pointer to the Path structure for the inner (right) side of the join
- `joinrestrictinfo`: List of RestrictInfo structures representing join conditions that must be evaluated during the join

## Dependencies
- Functions called/Symbols referenced:
  - [Path](../P/Path.md) (base structure)
  - JoinType (enumeration for join types)
  - [List](../L/List.md) (PostgreSQL list structure)
  - [RestrictInfo](../R/RestrictInfo.md) (via joinrestrictinfo list)

- Called from (representative examples):
  - [GetExistingLocalJoinPath](../G/GetExistingLocalJoinPath.md) (foreign data wrapper support)
  - cost_qual_eval_context (cost estimation)
  - [create_join_plan](../c/create_join_plan.md) (plan creation)
  - [has_indexed_join_quals](../h/has_indexed_join_quals.md) (join optimization)
  - [NestPath](../N/NestPath.md), MergePath, HashPath (concrete join path types)

## Notes and Other Information
- [JoinPath](JoinPath.md) is an abstract structure that is inherited by concrete join path types like NestPath, MergePath, and HashPath
- The inner_unique flag is critical for optimization decisions and is derived from uniqueness analysis of the inner relation
- The joinrestrictinfo cannot be merged into the parent RelOptInfo because join conditions are specific to the particular join being considered
- The structure supports foreign data wrapper integration through GetExistingLocalJoinPath
- [Join](Join.md) path selection involves comparing costs and capabilities of different join algorithms based on this common information
- The outer vs inner designation affects algorithm behavior and performance characteristics significantly