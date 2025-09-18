# SpecialJoinInfo

## Location
[src/include/optimizer/optimizer.h:44-44](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/optimizer/optimizer.h#L44-L44)

## Overview
SpecialJoinInfo represents information about special join types (outer joins, semi-joins, anti-joins) that require specific ordering constraints and optimization considerations during query planning.

## Definition
```c
struct SpecialJoinInfo
{
    pg_node_attr(no_read, no_query_jumble)

    NodeTag        type;
    Relids         min_lefthand;      /* base+OJ relids in minimum LHS for join */
    Relids         min_righthand;     /* base+OJ relids in minimum RHS for join */
    Relids         syn_lefthand;      /* base+OJ relids syntactically within LHS */
    Relids         syn_righthand;     /* base+OJ relids syntactically within RHS */
    JoinType       jointype;          /* always INNER, LEFT, FULL, SEMI, or ANTI */
    Index          ojrelid;           /* outer join's RT index; 0 if none */
    Relids         commute_above_l;   /* commuting OJs above this one, if LHS */
    Relids         commute_above_r;   /* commuting OJs above this one, if RHS */
    Relids         commute_below_l;   /* commuting OJs in this one's LHS */
    Relids         commute_below_r;   /* commuting OJs in this one's RHS */
    bool           lhs_strict;        /* joinclause is strict for some LHS rel */
    
    /* Remaining fields are set only for JOIN_SEMI jointype: */
    bool           semi_can_btree;    /* true if semi_operators are all btree */
    bool           semi_can_hash;     /* true if semi_operators are all hash */
    List          *semi_operators;    /* OIDs of equality join operators */
    List          *semi_rhs_exprs;    /* righthand-side expressions of these ops */
};
```

## Detailed Description
SpecialJoinInfo encodes the structural constraints and optimization opportunities for non-inner joins. It is created during query tree deconstruction and used throughout join planning to ensure semantic correctness and identify optimization opportunities.

The structure tracks both syntactic information (what relations appear textually on each side) and semantic constraints (minimum sets of relations required on each side). This distinction is crucial for outer join reordering, where syntactic position determines null-placement semantics but optimization may allow some relation reordering within semantic constraints.

For semi-joins, additional information about available join operators enables the planner to choose between hash-based and sort-based semi-join implementations.

## Parameters / Member Variables
### Core Join Information
- `type`: NodeTag for type identification
- `jointype`: Type of special join (LEFT, RIGHT, FULL, SEMI, ANTI)
- `ojrelid`: Range table index for the outer join RTE (0 if not applicable)

### Relation Set Constraints
- `min_lefthand`: Minimum set of base and outer-join relations required on left side
- `min_righthand`: Minimum set of base and outer-join relations required on right side
- `syn_lefthand`: Relations that syntactically appeared on the left side in original query
- `syn_righthand`: Relations that syntactically appeared on the right side in original query

### Commutation Constraints  
- `commute_above_l`: Outer joins above this one that can commute when this join is on left
- `commute_above_r`: Outer joins above this one that can commute when this join is on right
- `commute_below_l`: Outer joins contained within this join's left side
- `commute_below_r`: Outer joins contained within this join's right side

### Join Properties
- `lhs_strict`: True if join clause is strict (null-rejecting) for some left-hand relation

### Semi-Join Specific Information
- `semi_can_btree`: True if all semi-join equality operators support B-tree indexing
- `semi_can_hash`: True if all semi-join equality operators support hash indexing  
- `semi_operators`: List of OIDs for equality operators used in semi-join conditions
- `semi_rhs_exprs`: Right-hand expressions corresponding to each semi-join operator

## Dependencies
- Functions called/Symbols referenced:
  - Relids (relation set representation)
  - JoinType (enumeration of join types)
  - [List](../L/List.md) (generic list structure)
  - [RestrictInfo](../R/RestrictInfo.md) (join clause information)

- Called from (representative examples):
  - [make_outerjoininfo](../m/make_outerjoininfo.md)() (creates SpecialJoinInfo during query deconstruction)
  - [join_is_legal](../j/join_is_legal.md)() (uses SpecialJoinInfo to validate join ordering)
  - [add_paths_to_joinrel](../a/add_paths_to_joinrel.md)() (consults SpecialJoinInfo for join path generation)
  - [clause_selectivity](../c/clause_selectivity.md)() (uses SpecialJoinInfo for selectivity estimation)

## Notes and Other Information
SpecialJoinInfo is fundamental to PostgreSQL's handling of complex join queries. It ensures that outer join semantics are preserved during optimization while enabling maximum flexibility for join reordering within semantic constraints.

The distinction between min_* and syn_* relation sets is critical: min_* sets represent absolute requirements for correctness, while syn_* sets indicate the original syntactic structure. Join reordering is allowed as long as min_* constraints are satisfied.

Commutation constraints (commute_above_*, commute_below_*) handle the complex interactions between multiple outer joins in the same query. These enable safe reordering of outer joins when their effects don't interfere with each other.

Semi-join optimization relies heavily on the operator information (semi_can_btree, semi_can_hash, etc.) to choose the most efficient implementation method. Hash semi-joins are generally preferred when possible due to their typically superior performance characteristics.

The lhs_strict flag is crucial for outer join optimization, as strict clauses can sometimes be moved between join levels without changing query semantics, enabling additional optimization opportunities.