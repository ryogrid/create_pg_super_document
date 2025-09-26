# EquivalenceClass

## Location
[src/include/nodes/pathnodes.h:1379-1399](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L1379-L1399)

## Overview
EquivalenceClass represents a set of expressions that are known to be transitively equal to each other based on mergejoinable equality clauses, serving as the foundation for optimization decisions and PathKey generation.

## Definition
```c
typedef struct EquivalenceClass
{
    pg_node_attr(custom_read_write, no_copy_equal, no_read, no_query_jumble)

    NodeTag     type;

    List       *ec_opfamilies;      /* btree operator family OIDs */
    Oid         ec_collation;       /* collation, if datatypes are collatable */
    List       *ec_members;         /* list of EquivalenceMembers */
    List       *ec_sources;         /* list of generating RestrictInfos */
    List       *ec_derives;         /* list of derived RestrictInfos */
    Relids      ec_relids;          /* all relids appearing in ec_members, except for child members */
    bool        ec_has_const;       /* any pseudoconstants in ec_members? */
    bool        ec_has_volatile;    /* the (sole) member is a volatile expr */
    bool        ec_broken;          /* failed to generate needed clauses? */
    Index       ec_sortref;         /* originating sortclause label, or 0 */
    Index       ec_min_security;    /* minimum security_level in ec_sources */
    Index       ec_max_security;    /* maximum security_level in ec_sources */
    struct EquivalenceClass *ec_merged; /* set if merged into another EC */
} EquivalenceClass;
```

## Detailed Description
EquivalenceClass is a central data structure in PostgreSQL's query optimizer that represents sets of expressions known to be transitively equal according to mergejoinable equality clauses. When the optimizer encounters an equality clause like A = B that is not an outer-join clause, it creates or extends an EquivalenceClass to record this knowledge. Subsequent discoveries of related equalities (like B = C) extend the class, potentially triggering merges between existing classes.

The structure enforces strict constraints on equality relationships: all operators in an equivalence class must belong to the same set of btree operator families, and a single collation applies to all collatable datatypes within the class. This ensures semantic consistency for equality operations.

EquivalenceClasses respect join domain boundaries, meaning deductions only hold within sets of relations that are inner-joined together. The structure handles this through careful management of Var nullingrel sets and explicit JoinDomain tracking for pseudoconstant expressions, preventing false merges across join domains.

Beyond representing equality relationships, EquivalenceClasses serve as the foundation for PathKeys, enabling the optimizer to understand when different sort orderings are equivalent. Single-member classes often arise from sort expressions that haven't been equivalenced to other expressions, including special cases like volatile expressions (e.g., ORDER BY random()).

The optimization process uses EquivalenceClasses to generate implied equality clauses, optimize join orders, eliminate redundant sorts, and make various cost-based decisions. The structure's design ensures these optimizations remain semantically correct across complex query structures involving outer joins and other advanced SQL features.

## Parameters / Member Variables
- `type`: Standard NodeTag for node type identification
- `ec_opfamilies`: List of btree operator family OIDs defining equality semantics for this class
- `ec_collation`: Collation OID for collatable datatypes, ensuring consistent comparison semantics
- `ec_members`: List of EquivalenceMembers representing all expressions known to be equal
- `ec_sources`: List of RestrictInfo nodes that generated this equivalence class
- `ec_derives`: List of RestrictInfo nodes derived from this equivalence class
- `ec_relids`: Bitmap of all relation IDs appearing in ec_members (excluding child members)
- `ec_has_const`: Boolean flag indicating presence of pseudoconstant expressions in the class
- `ec_has_volatile`: Boolean flag indicating the sole member is a volatile expression
- `ec_broken`: Boolean flag indicating failure to generate required clauses
- `ec_sortref`: SortGroupRef from originating sort clause (0 if not from ORDER BY)
- `ec_min_security`: Minimum security level among source RestrictInfo nodes
- `ec_max_security`: Maximum security level among source RestrictInfo nodes
- `ec_merged`: Pointer to another EquivalenceClass if this one has been merged (should be ignored if not NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [EquivalenceClass](EquivalenceClass.md) (self-reference for ec_merged)
  - [List](../L/List.md) (generic list structure)
  - Relids (relation ID bitmap)

- Called from (representative examples):
  - [process_equivalence](../p/process_equivalence.md) (equivclass.c:132, 245, 409)
  - [add_eq_member](../a/add_eq_member.md) (equivclass.c:516, 585)
  - [get_eclass_for_sort_expr](../g/get_eclass_for_sort_expr.md) (equivclass.c:597, 618, 669)
  - [generate_base_implied_equalities](../g/generate_base_implied_equalities.md) (equivclass.c:1045)
  - [generate_join_implied_equalities_for_ecs](../g/generate_join_implied_equalities_for_ecs.md) (equivclass.c:1506)
  - [make_canonical_pathkey](../m/make_canonical_pathkey.md) (pathkeys.c:56)
  - [select_outer_pathkeys_for_merge](../s/select_outer_pathkeys_for_merge.md) (pathkeys.c:1645, 1659, 1666)
  - [create_mergejoin_plan](../c/create_mergejoin_plan.md) (createplan.c:4458, 4596, 4597, 4599)

## Notes and Other Information
- EquivalenceClasses are never copied after creation, enabling efficient pointer-based equality comparisons
- The ec_merged mechanism allows for efficient class consolidation without expensive data structure updates
- Volatile expressions receive special handling to ensure correctness in ORDER BY scenarios with multiple volatile expressions
- The structure's design prevents inappropriate optimization across outer join boundaries through careful join domain management
- Security level tracking ensures that security-sensitive operations maintain appropriate access controls
- The broken flag helps the optimizer recover gracefully when constraint generation fails
- EquivalenceClasses are fundamental to many optimization techniques including join reordering, index selection, and sort elimination
- The structure supports both simple Var-to-Var equalities and complex expression equivalences