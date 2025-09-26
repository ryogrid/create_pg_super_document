# Join

## Location
[src/include/nodes/plannodes.h:786-794](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L786-L794)

## Overview
Join is an abstract base plan node type for all join operations in PostgreSQL, providing common structure and semantics for combining tuples from two input relations.

## Definition
```c
typedef struct Join
{
    pg_node_attr(abstract)

    Plan        plan;
    JoinType    jointype;
    bool        inner_unique;
    List       *joinqual;       /* JOIN quals (in addition to plan.qual) */
} Join;
```

## Detailed Description
Join serves as the abstract base structure for all join plan node types in PostgreSQL (NestLoop, MergeJoin, HashJoin). It extends the base Plan structure with join-specific information including the type of join operation, optimization hints, and join qualification conditions. The structure distinguishes between join qualifications (joinqual) which determine tuple matching and general qualifications (plan.qual) which filter the final result. For outer joins, this distinction is crucial as joinqual determines when to generate null-extended tuples, while plan.qual is applied after tuple formation. The inner_unique flag provides an important optimization hint indicating that each outer tuple can match at most one inner tuple, allowing executors to skip searching for additional matches.

## Parameters / Member Variables
- `plan`: Base Plan structure containing common plan node fields (type, cost, target list, etc.)
- `jointype`: JoinType enum specifying join semantics (INNER, LEFT, RIGHT, FULL, SEMI, ANTI)
- `inner_unique`: Boolean optimization flag indicating if each outer tuple matches at most one inner tuple
- `joinqual`: List of join qualification expressions that determine tuple matching (separate from WHERE conditions in plan.qual)

## Dependencies
- Functions called/Symbols referenced:
  - [Plan](../P/Plan.md) (base structure)
  - JoinType (join type enumeration)
  - [List](../L/List.md) (container type)
- Called from (representative examples):
  - [NestLoop](../N/NestLoop.md) (inherits from Join)
  - [MergeJoin](../M/MergeJoin.md) (inherits from Join)
  - [HashJoin](../H/HashJoin.md) (inherits from Join)
  - [ExplainNode](../E/ExplainNode.md) (EXPLAIN output)
  - [set_join_references](../s/set_join_references.md) (plan reference fixing)
  - [finalize_plan](../f/finalize_plan.md) (plan finalization)

## Notes and Other Information
- Abstract base class - never instantiated directly, only through concrete subclasses
- Critical distinction between joinqual and plan.qual for outer join semantics
- joinqual determines match detection for null-extension in outer joins
- plan.qual applied after tuple formation and null-extension
- inner_unique optimization allows early termination of inner relation scans
- For INNER joins, joinqual and plan.qual are semantically equivalent
- Only joinquals can be used as merge or hash conditions in specialized join algorithms
- The pg_node_attr(abstract) annotation prevents direct instantiation
- All concrete join implementations (NestLoop, MergeJoin, HashJoin) inherit this structure
- [Join](Join.md) qualification expressions are typically equality conditions but can be more complex