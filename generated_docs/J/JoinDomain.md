# JoinDomain

## Location
[src/include/nodes/pathnodes.h:1317-1324](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L1317-L1324)

## Overview
JoinDomain defines the scope of applicability of deductions made via the EquivalenceClass mechanism, representing a set of base and outer join relations that are inner-joined together.

## Definition
```c
typedef struct JoinDomain
{
    pg_node_attr(no_copy_equal, no_read, no_query_jumble)

    NodeTag     type;

    Relids      jd_relids;      /* all relids contained within the domain */
} JoinDomain;
```

## Detailed Description
JoinDomain is a fundamental concept in PostgreSQL's query optimization that defines the scope within which equality deductions from EquivalenceClasses can be enforced or expected to hold. It represents a set of relations that are connected by inner joins, establishing boundaries for where equivalence-based optimizations can be safely applied.

The structure is designed to handle the complexities introduced by outer joins, which create semantic boundaries where equality relationships may not hold consistently. The topmost JoinDomain covers the entire query (with jd_relids equaling all_query_rels), while outer joins create nested domains with more restricted scope.

Outer joins create new JoinDomains that include all base and outer join relation IDs within the nullable side, but exclude the outer join's own relation ID by convention. FULL joins create two separate JoinDomains, one for each side. This hierarchical structure enables the optimizer to reason about where specific equality conditions can be safely applied without violating the semantics of outer join operations.

Relations below outer joins may appear in multiple join domains, but nullingrel bits prevent inappropriate evaluation of equivalence class constraints at levels where they shouldn't be enforced.

## Parameters / Member Variables
- `type`: Standard NodeTag for node type identification
- `jd_relids`: Relids (relation ID bitmap) containing all relation identifiers that belong to this join domain

## Dependencies
- Functions called/Symbols referenced:
  - Relids (bitmap set for relation IDs)

- Called from (representative examples):
  - [process_equivalence](../p/process_equivalence.md) (equivclass.c:119)
  - [add_eq_member](../a/add_eq_member.md) (equivclass.c:517)
  - [get_eclass_for_sort_expr](../g/get_eclass_for_sort_expr.md) (equivclass.c:595, 611)
  - [reconsider_outer_join_clause](../r/reconsider_outer_join_clause.md) (equivclass.c:2195)
  - [find_join_domain](../f/find_join_domain.md) (equivclass.c:2426)
  - [deconstruct_jointree](../d/deconstruct_jointree.md) (initsplan.c:743, 755)
  - [deconstruct_recurse](../d/deconstruct_recurse.md) (initsplan.c:823, 909, 944, 1012, 1016, 1025)
  - [distribute_qual_to_rels](../d/distribute_qual_to_rels.md) (initsplan.c:2310)
  - [subquery_planner](../s/subquery_planner.md) (planner.c:688)

## Notes and Other Information
- JoinDomains are computed during the deconstruct_jointree phase of query planning
- These structures are never copied after creation, enabling equality comparison through simple pointer equality
- The domain hierarchy allows determination of "higher" or "lower" domains based on relid set inclusion relationships
- The design supports the EquivalenceClass mechanism by clearly defining where equality deductions can be safely applied
- JoinDomains are essential for handling the semantic complexities introduced by various types of outer joins
- The structure enables proper scoping of equivalence relationships in the presence of nullable and non-nullable sides of outer joins
- This mechanism prevents the optimizer from making invalid deductions that would produce incorrect query results in outer join scenarios