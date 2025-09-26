# Join

## Location
src/include/nodes/plannodes.h: 786 - 794

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
  - Plan (base structure)
  - JoinType (join type enumeration)
  - List (container type)
- Called from (representative examples):
  - NestLoop (inherits from Join)
  - MergeJoin (inherits from Join)
  - HashJoin (inherits from Join)
  - ExplainNode (EXPLAIN output)
  - set_join_references (plan reference fixing)
  - finalize_plan (plan finalization)

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
- Join qualification expressions are typically equality conditions but can be more complex