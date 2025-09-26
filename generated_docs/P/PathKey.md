# PathKey

## Location
[src/include/nodes/pathnodes.h:1463-1474](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L1463-L1474)

## Overview
PathKey represents a single sort ordering component within PostgreSQL's query optimization, defining how values are ordered by referencing an EquivalenceClass and specifying the sort direction and null handling.

## Definition
```c
typedef struct PathKey
{
    pg_node_attr(no_read, no_query_jumble)

    NodeTag     type;

    EquivalenceClass *pk_eclass;    /* the value that is ordered */
    Oid         pk_opfamily;        /* btree opfamily defining the ordering */
    int         pk_strategy;        /* sort direction (ASC or DESC) */
    bool        pk_nulls_first;     /* do NULLs come before normal values? */
} PathKey;
```

## Detailed Description
PathKey is a fundamental building block in PostgreSQL's representation of sort orderings. A list of PathKey nodes represents the complete sort specification for a path, where an empty list implies no known ordering, and each PathKey in the list represents a sort key in order of precedence (primary, secondary, etc.).

The design leverages the EquivalenceClass system to achieve efficient ordering comparison and optimization. By linking to an EquivalenceClass rather than directly to expressions, PathKeys make it trivial to detect equivalent and closely-related orderings. For example, if expressions A and B are in the same EquivalenceClass, then sorting by A ascending is equivalent to sorting by B ascending.

The structure includes all necessary information to fully specify a sort ordering: the EquivalenceClass identifies what values are being sorted, the opfamily defines the comparison operators, the strategy specifies ascending versus descending order, and the nulls_first flag determines NULL value placement.

This representation enables powerful optimizations such as recognizing when an existing sort order can satisfy a required ordering, determining when sorts can be eliminated, and efficiently comparing different ordering requirements across query plans.

## Parameters / Member Variables
- `type`: Standard NodeTag for node type identification
- `pk_eclass`: Pointer to EquivalenceClass containing the expression being sorted, with copy_as_scalar and equal_as_scalar attributes for efficient pointer-based operations
- `pk_opfamily`: OID of btree operator family defining the ordering semantics and comparison operators
- `pk_strategy`: Integer strategy number indicating sort direction (BTLessStrategyNumber for ASC, BTGreaterStrategyNumber for DESC)
- `pk_nulls_first`: Boolean flag indicating whether NULL values should appear before normal values in the sort order

## Dependencies
- Functions called/Symbols referenced:
  - EquivalenceClass (structure for equivalence classes)

- Called from (representative examples):
  - make_canonical_pathkey (pathkeys.c:59, 73, 87)
  - compare_pathkeys (pathkeys.c:317, 318)
  - pathkey_is_redundant (pathkeys.c:158, 170, 196)
  - build_index_pathkeys (pathkeys.c:756)
  - make_pathkeys_for_sortclauses_extended (pathkeys.c:1386)
  - select_outer_pathkeys_for_merge (pathkeys.c:1718, 1739, 1781)
  - create_mergejoin_plan (createplan.c:4457, 4598, 4636, 4660, 4678)
  - initial_cost_mergejoin (costsize.c:3558, 3559, 3567, 3568)

## Notes and Other Information
- PathKey nodes assume all ordering-capable index types use btree-compatible strategy numbers for consistency
- The copy_as_scalar and equal_as_scalar attributes on pk_eclass enable efficient pointer-based equality comparisons without deep copying
- Lists of PathKeys represent complete sort orderings where order matters (first PathKey is primary sort, second is secondary, etc.)
- Empty PathKey lists indicate no known or required ordering
- The structure's design makes it efficient to determine ordering compatibility between different query plan nodes
- PathKeys are central to sort elimination optimizations, merge join planning, and index selection decisions
- The EquivalenceClass linkage allows the optimizer to recognize when different expressions can produce equivalent orderings
- NULL handling is explicitly controlled through pk_nulls_first, supporting both NULLS FIRST and NULLS LAST semantics
- The opfamily mechanism allows support for custom sort orderings beyond standard comparison operators