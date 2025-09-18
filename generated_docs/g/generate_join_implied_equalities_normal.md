# generate_join_implied_equalities_normal

## Location
src/backend/optimizer/path/equivclass.c: 1547 - 1722

## Overview
Generates join-implied equality clauses for a non-broken EquivalenceClass by optimally selecting member pairs and creating appropriate join conditions.

## Definition
```c
static List *generate_join_implied_equalities_normal(PlannerInfo *root, EquivalenceClass *ec, Relids join_relids, Relids outer_relids, Relids inner_relids)
```

## Detailed Description
This function implements the core logic for generating join clauses from a healthy (non-broken) EquivalenceClass. It operates in two main phases:

**Phase 1 - Member Classification**: Categorizes EC members into three groups:
- `outer_members`: Members computable at the outer relation
- `inner_members`: Members computable at the inner relation  
- `new_members`: Members newly computable at this join level

**Phase 2 - Clause Generation**: 
- Creates one join clause between the best outer and inner member pair using a scoring system that prefers:
  - Simple Var expressions (with optional RelabelType)
  - Hash-joinable operators
  - Compatible data types
- Generates additional clauses to connect all new_members to existing members

The function uses sophisticated optimization to select the best member combinations, considering both runtime performance and statistical quality. It marks certain clauses as redundant (via parent_ec) to avoid duplicate enforcement.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state and operator information
- `ec`: The EquivalenceClass to process for join clause generation
- `join_relids`: Bitmap representing all relations involved in the join operation
- `outer_relids`: Bitmap of relations from the outer side of the join
- `inner_relids`: Bitmap of relations from the inner side of the join

## Dependencies
- Functions called/Symbols referenced:
  - bms_is_subset
  - select_equality_operator
  - op_hashjoinable
  - create_join_clause
  - list_concat
- Called from (representative examples):
  - generate_join_implied_equalities (src/backend/optimizer/path/equivclass.c:1446)
  - generate_join_implied_equalities_for_ecs (src/backend/optimizer/path/equivclass.c:1522)

## Notes and Other Information
- Sets ec_broken flag and returns NIL if no compatible equality operator can be found
- Uses a scoring system (0-3) to select optimal member pairs for join conditions
- Handles child EC members implicitly through join_relids subset testing
- Creates redundant clauses (marked with parent_ec) for optimization and non-redundant clauses for completeness
- Follows a left-to-right chaining strategy for connecting new members
- Part of PostgreSQL's equivalence class-based join optimization system
- Critical for generating efficient join plans in complex multi-table queries