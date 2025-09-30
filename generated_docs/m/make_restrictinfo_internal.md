# make_restrictinfo_internal

## Location
[src/backend/optimizer/util/restrictinfo.c:112-270](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/restrictinfo.c#L112-L270)

## Overview
Internal common implementation for creating RestrictInfo nodes, handling the detailed initialization of all RestrictInfo fields including relation dependencies, join capabilities, and performance optimization caches.

## Definition

```c
union(restrictinfo->left_relids,
												restrictinfo->right_relids);
```
## Detailed Description
This static function serves as the core implementation for RestrictInfo creation, performing comprehensive initialization of all RestrictInfo fields. It analyzes the clause structure to determine relation dependencies, evaluates join potential for binary operator clauses, handles security considerations including leak-proofness testing, and initializes performance-related caches. The function distinguishes between binary operator clauses (which may be join-capable) and other clause types, setting up appropriate left/right relation information for optimization purposes.

## Parameters / Member Variables
- : PlannerInfo structure containing planning context and global state
- : The primary expression being wrapped in the RestrictInfo
- : Parent OR clause if this RestrictInfo is part of an OR structure (can be NULL)
- : Flag indicating whether this restriction was pushed down from a higher query level
- : Flag indicating whether this RestrictInfo has associated clones
- : Flag indicating whether this RestrictInfo is itself a clone of another
- : Flag indicating whether the clause evaluates to a constant value
- : Security level for row-level security evaluation ordering
- : Explicit set of required relations (defaults to clause_relids if NULL)
- : Set of relations incompatible with this restriction
- : Set of relations that are outer to this restriction context

## Dependencies
- Functions called/Symbols referenced:
  - [contain_leaked_vars](../c/contain_leaked_vars.md)
  - VOLATILITY_UNKNOWN
  - [is_opclause](../i/is_opclause.md)
  - [OpExpr](../O/OpExpr.md)
  - [get_leftop](../g/get_leftop.md)
  - [get_rightop](../g/get_rightop.md)
  - [pull_varnos](../p/pull_varnos.md)
  - [bms_union](../b/bms_union.md)
  - bms_is_empty
  - [bms_overlap](../b/bms_overlap.md)
  - [bms_difference](../b/bms_difference.md)
  - [bms_num_members](../b/bms_num_members.md)
  - [bms_free](../b/bms_free.md)
- Called from (representative examples):
  - [make_restrictinfo](make_restrictinfo.md)
  - [make_sub_restrictinfos](make_sub_restrictinfos.md)

## Notes and Other Information
- Special handling for binary operator clauses: analyzes left and right operands separately to determine join capability and relation dependencies
- Security considerations: tests for leak-proofness when security_level > 0 to support row-level security
- Performance optimization: initializes numerous cache fields with sentinel values (-1, NIL, InvalidOid) that will be populated on-demand during query optimization
- [Join](../J/Join.md) detection: automatically identifies potential join clauses by checking if left and right operands reference disjoint sets of relations
- Base relation counting: calculates the number of base relations involved by excluding outer join relations from the clause's relation set
- Serial numbering: assigns a unique serial number to each RestrictInfo for debugging and tracking purposes
- Lazy evaluation design: most expensive computations (selectivity, join costs, etc.) are deferred until actually needed during optimization

## Simplified Source

```c
static RestrictInfo *
make_restrictinfo_internal(PlannerInfo *root, Expr *clause, Expr *orclause,
                          bool is_pushed_down, bool has_clone, bool is_clone,
                          bool pseudoconstant, Index security_level,
                          Relids required_relids, Relids incompatible_relids,
                          Relids outer_relids)
{
    RestrictInfo *restrictinfo = makeNode(RestrictInfo);
    Relids      baserels;

    // Initialize basic fields
    restrictinfo->clause = clause;
    restrictinfo->orclause = orclause;
    restrictinfo->is_pushed_down = is_pushed_down;
    restrictinfo->pseudoconstant = pseudoconstant;
    restrictinfo->has_clone = has_clone;
    restrictinfo->is_clone = is_clone;
    restrictinfo->can_join = false;
    restrictinfo->security_level = security_level;
    restrictinfo->incompatible_relids = incompatible_relids;
    restrictinfo->outer_relids = outer_relids;

    // Test for leak-proofness if security level > 0
    if (security_level > 0)
        restrictinfo->leakproof = !contain_leaked_vars((Node *) clause);
    else
        restrictinfo->leakproof = false;

    // Mark volatility as unknown (computed on demand)
    restrictinfo->has_volatile = VOLATILITY_UNKNOWN;

    // Handle binary operator clauses specially
    if (is_opclause(clause) && list_length(((OpExpr *) clause)->args) == 2)
    {
        restrictinfo->left_relids = pull_varnos(root, get_leftop(clause));
        restrictinfo->right_relids = pull_varnos(root, get_rightop(clause));
        restrictinfo->clause_relids = bms_union(restrictinfo->left_relids,
                                              restrictinfo->right_relids);

        // Check if this could be a join clause
        if (!bms_is_empty(restrictinfo->left_relids) &&
            !bms_is_empty(restrictinfo->right_relids) &&
            !bms_overlap(restrictinfo->left_relids, restrictinfo->right_relids))
        {
            restrictinfo->can_join = true;
        }
    }
    else
    {
        // Non-binary clause: no left/right split
        restrictinfo->left_relids = NULL;
        restrictinfo->right_relids = NULL;
        restrictinfo->clause_relids = pull_varnos(root, (Node *) clause);
    }

    // Set required_relids (defaults to clause_relids)
    if (required_relids != NULL)
        restrictinfo->required_relids = required_relids;
    else
        restrictinfo->required_relids = restrictinfo->clause_relids;

    // Count base relations (excluding outer joins)
    baserels = bms_difference(restrictinfo->clause_relids, root->outer_join_rels);
    restrictinfo->num_base_rels = bms_num_members(baserels);
    bms_free(baserels);

    // Assign unique serial number
    restrictinfo->rinfo_serial = ++(root->last_rinfo_serial);

    // Initialize cache fields with sentinel values
    restrictinfo->parent_ec = NULL;
    restrictinfo->eval_cost.startup = -1;
    restrictinfo->norm_selec = -1;
    restrictinfo->outer_selec = -1;
    restrictinfo->mergeopfamilies = NIL;
    restrictinfo->left_ec = NULL;
    restrictinfo->right_ec = NULL;
    restrictinfo->left_em = NULL;
    restrictinfo->right_em = NULL;
    restrictinfo->scansel_cache = NIL;
    restrictinfo->outer_is_left = false;
    restrictinfo->hashjoinoperator = InvalidOid;
    restrictinfo->left_bucketsize = -1;
    restrictinfo->right_bucketsize = -1;
    restrictinfo->left_mcvfreq = -1;
    restrictinfo->right_mcvfreq = -1;
    restrictinfo->left_hasheqoperator = InvalidOid;
    restrictinfo->right_hasheqoperator = InvalidOid;

    return restrictinfo;
}
```