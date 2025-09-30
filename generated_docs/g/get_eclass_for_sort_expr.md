# get_eclass_for_sort_expr

## Location
[src/backend/optimizer/path/equivclass.c:586-758](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/equivclass.c#L586-L758)

## Overview
Finds an existing EquivalenceClass containing a given sort expression, or optionally creates a new single-member EquivalenceClass for expressions not yet represented.

## Definition

```c
struct the EC in the right context.
	 */
	oldcontext = MemoryContextSwitchTo(root->planner_cxt);
```
## Detailed Description
This function is used to integrate sort expressions (from ORDER BY, GROUP BY, etc.) into the EquivalenceClass system. It first searches existing EquivalenceClasses for a match based on expression equality, operator families, and collation. If no match is found and create_it is true, it constructs a new EquivalenceClass.

The function handles several important considerations:

1. **Volatile Expression Handling**: Volatile expressions are only matched when they share the same SortGroupRef, ensuring consistent behavior across references to the same volatile expression.

2. **Child Member Matching**: When rel is specified, child EquivalenceMembers for that specific relation are considered in addition to regular members.

3. **JoinDomain Restrictions**: Constants are only matched within the same JoinDomain to maintain proper scoping.

4. **Expression Validation**: Unlike process_equivalence(), this function must validate that expressions marked as constants don't contain volatile functions, aggregates, set-returning functions, or window functions.

5. **EC Index Maintenance**: When merging is complete, newly created EquivalenceClasses are properly indexed in relation structures for efficient lookup.

## Parameters / Member Variables
- : PlannerInfo containing global optimizer state and existing EquivalenceClasses
- : Sort expression to find or create an EquivalenceClass for
- : List of btree operator families for the expression
- : Input type for the operator class
- : Required collation for the expression
- : SortGroupRef identifier from the originating clause (required for volatile expressions)
- : Specific relation to consider child members for, or NULL to ignore child members
- : Whether to create a new EquivalenceClass if no match is found

## Dependencies
- Functions called/Symbols referenced:
  - [canonicalize_ec_expression](../c/canonicalize_ec_expression.md)
  - linitial_node
  - [equal](../e/equal.md)
  - [bms_equal](../b/bms_equal.md)
  - makeNode
  - [list_copy](../l/list_copy.md)
  - [contain_volatile_functions](../c/contain_volatile_functions.md)
  - [pull_varnos](../p/pull_varnos.md)
  - [add_eq_member](../a/add_eq_member.md)
  - copyObject
  - [expression_returns_set](../e/expression_returns_set.md)
  - [contain_agg_clause](../c/contain_agg_clause.md)
  - [contain_window_function](../c/contain_window_function.md)
  - [bms_next_member](../b/bms_next_member.md)
  - [bms_add_member](../b/bms_add_member.md)
- Called from (representative examples):
  - [make_pathkey_from_sortinfo](../m/make_pathkey_from_sortinfo.md)
  - [convert_subquery_pathkeys](../c/convert_subquery_pathkeys.md)
  - [initialize_mergeclause_eclasses](../i/initialize_mergeclause_eclasses.md)

## Notes and Other Information
- Safe to use both before and after EquivalenceClass merging since it never causes merging
- Child member matching can be order-dependent when multiple ECs match the same expression
- Volatile EquivalenceClasses require a valid sortref and cannot be matched without one
- New EquivalenceClasses are constructed in the planner memory context for proper lifespan
- Expression canonicalization ensures consistent type and collation exposure
- Returns NULL when no match is found and create_it is false

## Simplified Source

```c
EquivalenceClass *get_eclass_for_sort_expr(PlannerInfo *root, Expr *expr, List *opfamilies,
                                          Oid opcintype, Oid collation, Index sortref,
                                          Relids rel, bool create_it) {
    JoinDomain *jdomain;
    Relids expr_relids;
    EquivalenceClass *newec;
    EquivalenceMember *newem;
    ListCell *lc1;
    MemoryContext oldcontext;

    // Canonicalize the expression type and collation
    expr = canonicalize_ec_expression(expr, opcintype, collation);

    // Use the top-level join domain for sort expressions
    jdomain = linitial_node(JoinDomain, root->join_domains);

    // Search existing equivalence classes for a match
    foreach(lc1, root->eq_classes) {
        EquivalenceClass *cur_ec = (EquivalenceClass *) lfirst(lc1);
        ListCell *lc2;

        // Skip volatile ECs unless sortref matches
        if (cur_ec->ec_has_volatile && (sortref == 0 || sortref != cur_ec->ec_sortref))
            continue;

        // Check collation and operator families
        if (collation != cur_ec->ec_collation || !equal(opfamilies, cur_ec->ec_opfamilies))
            continue;

        // Check each member for expression match
        foreach(lc2, cur_ec->ec_members) {
            EquivalenceMember *cur_em = (EquivalenceMember *) lfirst(lc2);

            // Skip child members unless they match the request
            if (cur_em->em_is_child && !bms_equal(cur_em->em_relids, rel))
                continue;

            // Match constants only within the same JoinDomain
            if (cur_em->em_is_const && cur_em->em_jdomain != jdomain)
                continue;

            // Check for expression and type match
            if (opcintype == cur_em->em_datatype && equal(expr, cur_em->em_expr))
                return cur_ec;  // Found match
        }
    }

    // No match found
    if (!create_it)
        return NULL;

    // Create new single-member EquivalenceClass
    oldcontext = MemoryContextSwitchTo(root->planner_cxt);

    newec = makeNode(EquivalenceClass);
    newec->ec_opfamilies = list_copy(opfamilies);
    newec->ec_collation = collation;
    newec->ec_members = NIL;
    newec->ec_sources = NIL;
    newec->ec_derives = NIL;
    newec->ec_relids = NULL;
    newec->ec_has_const = false;
    newec->ec_has_volatile = contain_volatile_functions((Node *) expr);
    newec->ec_broken = false;
    newec->ec_sortref = sortref;
    newec->ec_min_security = UINT_MAX;
    newec->ec_max_security = 0;
    newec->ec_merged = NULL;

    // Validate volatile expressions have sortref
    if (newec->ec_has_volatile && sortref == 0)
        elog(ERROR, "volatile EquivalenceClass has no sortref");

    // Add the expression as a member
    expr_relids = pull_varnos(root, (Node *) expr);
    newem = add_eq_member(newec, copyObject(expr), expr_relids, jdomain, NULL, opcintype);

    // Validate const marking for expressions with prohibited constructs
    if (newec->ec_has_const) {
        if (newec->ec_has_volatile || expression_returns_set((Node *) expr) ||
            contain_agg_clause((Node *) expr) || contain_window_function((Node *) expr)) {
            newec->ec_has_const = false;
            newem->em_is_const = false;
        }
    }

    root->eq_classes = lappend(root->eq_classes, newec);

    // Update relation indexes if EC merging is complete
    if (root->ec_merging_done) {
        int ec_index = list_length(root->eq_classes) - 1;
        int i = -1;

        while ((i = bms_next_member(newec->ec_relids, i)) > 0) {
            RelOptInfo *rel_info = root->simple_rel_array[i];
            if (rel_info && rel_info->reloptkind == RELOPT_BASEREL) {
                rel_info->eclass_indexes = bms_add_member(rel_info->eclass_indexes, ec_index);
            }
        }
    }

    MemoryContextSwitchTo(oldcontext);
    return newec;
}
```