# build_joinrel_joinlist

## Location
[src/backend/optimizer/util/relnode.c:1334-1351](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/relnode.c#L1334-L1351)

## Overview
Builds the joininfo list for a new join relation by collecting joininfo clauses from both outer and inner input relations.

## Definition

```c
static void
build_joinrel_joinlist(RelOptInfo *joinrel,
					   RelOptInfo *outer_rel,
					   RelOptInfo *inner_rel)
```
## Detailed Description
The  function is responsible for constructing the joininfo list for a newly created join relation. It collects all join clauses that syntactically belong above the current join level from both the outer and inner input relations. The function eliminates duplicates since many of the same clauses may arrive from both input relations.

The function works by calling  twice - once for each input relation's joininfo list - and accumulates the results. The final result is stored in the joinrel's joininfo field.

## Parameters / Member Variables
- : The new join relation being constructed that will receive the combined joininfo list
- : The outer input relation whose joininfo clauses will be collected
- : The inner input relation whose joininfo clauses will be collected

## Dependencies
- Functions called/Symbols referenced:
  - subbuild_joinrel_joinlist
- Called from (representative examples):
  - build_join_rel

## Notes and Other Information
- This is a static function within relnode.c, used internally for join relation construction
- The function is part of the join relation building process in PostgreSQL's query optimizer
- Duplicate elimination is important because the same join clauses may be present in both input relations' joininfo lists
- The function operates at lines 1334-1351 in src/backend/optimizer/util/relnode.c