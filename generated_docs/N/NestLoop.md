# NestLoop

## Location
[src/include/nodes/plannodes.h:807-811](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L807-L811)

## Overview
NestLoop is a concrete join plan node that implements nested loop join algorithm, executing the inner relation once for each tuple from the outer relation with parameterized execution.

## Definition
```c
typedef struct NestLoop
{
    Join        join;
    List       *nestParams;     /* list of NestLoopParam nodes */
} NestLoop;
```

## Detailed Description
NestLoop implements the nested loop join algorithm, one of the fundamental join methods in PostgreSQL. It inherits from the abstract Join base structure and adds parameterization support through the nestParams list. The algorithm works by iterating through each tuple in the outer relation and for each outer tuple, executing the inner relation's plan with parameters passed from the outer tuple. This parameterization mechanism allows the inner plan to use values from the current outer tuple, enabling efficient execution of correlated subqueries and joins with complex predicates. The nestParams list contains NestLoopParam structures that specify which outer relation values should be passed as parameters to the inner relation's execution. This join method is particularly effective for small outer relations or when the inner relation can be efficiently accessed via an index using the parameterized values.

## Parameters / Member Variables
- `join`: Base Join structure containing common join fields (Plan plan, JoinType jointype, bool inner_unique, List *joinqual)
- `nestParams`: List of NestLoopParam nodes specifying parameter passing from outer to inner relation during execution

## Dependencies
- Functions called/Symbols referenced:
  - Join (base structure)
  - List (container type)
  - NestLoopParam (parameter specification structure)
- Called from (representative examples):
  - ExecInitNode (plan node initialization dispatcher)
  - ExecNestLoop (main execution function)
  - ExecInitNestLoop (initialization function)
  - create_nestloop_plan (plan creation)
  - make_nestloop (plan node construction)
  - set_join_references (plan reference fixing)
  - ExplainNode (EXPLAIN output)

## Notes and Other Information
- Implements the nested loop join algorithm with parameterization support
- Most flexible join algorithm - can handle any join condition
- Performance characteristics: O(M * N) where M is outer relation size, N is inner relation size
- Particularly efficient when outer relation is small and inner relation can use indexes
- The nestParams mechanism enables correlated execution between outer and inner plans
- Parameters are restricted to simple Vars during execution (PlaceHolderVars allowed during planning)
- Parameter values must have varno OUTER_VAR by execution time
- Supports all join types (INNER, LEFT, RIGHT, FULL, SEMI, ANTI)
- Can be the fallback choice when other join algorithms (hash, merge) are not applicable
- Memory usage is typically low as it processes one outer tuple at a time
- Often chosen for joins with complex non-equijoin conditions