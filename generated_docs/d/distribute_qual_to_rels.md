# distribute_qual_to_rels

## Location
src/backend/optimizer/plan/initsplan.c: 2197 - 2583

## Overview
Distributes a qualification clause to appropriate base relations by creating RestrictInfo nodes and adding them to baserestrictinfo or joininfo lists, or processes them through the EquivalenceClass machinery.

## Definition
```c
static void distribute_qual_to_rels(PlannerInfo *root, Node *clause,
                                   JoinTreeItem *jtitem,
                                   SpecialJoinInfo *sjinfo,
                                   Index security_level,
                                   Relids qualscope,
                                   Relids ojscope,
                                   Relids outerjoin_nonnullable,
                                   Relids incompatible_relids,
                                   bool allow_equivalence,
                                   bool has_clone,
                                   bool is_clone,
                                   List **postponed_oj_qual_list)
```

## Detailed Description
This is a core function in PostgreSQL's query planning process that handles the distribution of qualification clauses (WHERE conditions, JOIN conditions) to appropriate relations. The function performs several critical tasks:

1. **Scope Analysis**: Determines which relations are referenced by the clause and validates scope constraints
2. **Lateral Reference Handling**: Manages clauses with LATERAL subquery references that extend beyond normal scope
3. **Outer Join Processing**: Handles special semantics for outer join qualifications
4. **Pseudoconstant Detection**: Identifies clauses that can be evaluated once and used as gating conditions
5. **EquivalenceClass Integration**: Routes mergejoinable clauses to the equivalence class machinery
6. **RestrictInfo Creation**: Builds RestrictInfo nodes with appropriate metadata
7. **Clause Distribution**: Places clauses in the correct restriction or join lists

The function implements complex logic for determining whether clauses should be pushed down to base relations, kept at join levels, or processed through equivalence classes.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state
- `clause`: The qualification clause to be distributed
- `jtitem`: JoinTreeItem representing the containing jointree node context
- `sjinfo`: SpecialJoinInfo for outer joins (NULL for inner joins/WHERE clauses)
- `security_level`: Security level to assign to the qualification for row-level security
- `qualscope`: Set of base+OJ relations the clause's syntactic scope covers
- `ojscope`: Minimum set of relations needed to form an outer join (NULL for non-OJ clauses)
- `outerjoin_nonnullable`: Relations on the non-nullable side of outer joins
- `incompatible_relids`: Outer-join relations that must not be computed below this clause
- `allow_equivalence`: Whether the clause can be converted to an EquivalenceClass
- `has_clone`: Whether the qualification has clone properties
- `is_clone`: Whether this qualification is a clone
- `postponed_oj_qual_list`: Output list for outer join clauses requiring later processing

## Dependencies
- Functions called/Symbols referenced:
  - pull_varnos
  - check_redundant_nullability_qual
  - make_restrictinfo
  - process_equivalence
  - initialize_mergeclause_eclasses
  - distribute_restrictinfo_to_rels
  - check_mergejoinable
  - add_vars_to_targetlist
- Called from (representative examples):
  - distribute_quals_to_rels

## Notes and Other Information
This function implements sophisticated logic for handling different types of qualifications:
- **Degenerate outer join clauses** are treated as regular filter conditions
- **Non-degenerate outer join clauses** must be evaluated at the outer join level
- **Pseudoconstant clauses** are marked for potential gating Result node creation
- **Mergejoinable clauses** are routed through equivalence class processing when appropriate
- The function handles the complex interplay between syntactic scope, semantic requirements, and optimization opportunities in PostgreSQL's cost-based optimizer.