# make_restrictinfo

## Location
[src/backend/optimizer/util/restrictinfo.c:63-111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/restrictinfo.c#L63-L111)

## Overview
Creates a RestrictInfo node containing a given subexpression, with proper handling of OR clauses and delegation to make_restrictinfo_internal for standard processing.

## Definition

```c
structure.
	 */
	if (is_orclause(clause))
		return (RestrictInfo *) make_sub_restrictinfos(root,
													   clause,
													   is_pushed_down,
													   has_clone,
													   is_clone,
													   pseudoconstant,
													   security_level,
													   required_relids,
													   incompatible_relids,
													   outer_relids);
```
## Detailed Description
The make_restrictinfo function serves as the primary entry point for creating RestrictInfo nodes that wrap query restriction clauses. It performs special handling for OR clauses by delegating to make_sub_restrictinfos to recursively process the OR structure, while standard clauses are passed to make_restrictinfo_internal. The function requires the caller to provide various flags and metadata about the restriction clause, including security level, relation dependencies, and behavioral characteristics.

## Parameters / Member Variables
- : PlannerInfo structure containing planning context and state
- : The expression to be wrapped in a RestrictInfo node
- : Flag indicating whether this restriction was pushed down from a higher level
- : Flag indicating whether this RestrictInfo has clones
- : Flag indicating whether this RestrictInfo is itself a clone
- : Flag indicating whether the clause is a pseudoconstant
- : Security level for row-level security considerations
- : Set of relation IDs that must be present for this restriction (can be NULL)
- : Set of relation IDs that are incompatible with this restriction
- : Set of relation IDs that are outer to this restriction

## Dependencies
- Functions called/Symbols referenced:
  - [is_orclause](../i/is_orclause.md)
  - [make_sub_restrictinfos](make_sub_restrictinfos.md)
  - [is_andclause](../i/is_andclause.md)
  - [make_restrictinfo_internal](make_restrictinfo_internal.md)
- Called from (representative examples):
  - [process_equivalence](../p/process_equivalence.md)
  - [reconsider_outer_join_clauses](../r/reconsider_outer_join_clauses.md)
  - [distribute_qual_to_rels](../d/distribute_qual_to_rels.md)
  - [add_base_clause_to_rel](../a/add_base_clause_to_rel.md)
  - [process_implied_equality](../p/process_implied_equality.md)
  - [build_implied_join_equality](../b/build_implied_join_equality.md)
  - [apply_child_basequals](../a/apply_child_basequals.md)
  - [add_join_clause_to_rels](../a/add_join_clause_to_rels.md)
  - [consider_new_or_clause](../c/consider_new_or_clause.md)
  - make_simple_restrictinfo

## Notes and Other Information
- Special handling for OR clauses: when an OR clause is encountered, it delegates to make_sub_restrictinfos which recursively processes the OR structure with RestrictInfo nodes
- Includes an assertion that the clause should not be an AND clause, as AND/OR flattening should have handled this case earlier in processing
- The function initializes only fields that depend on the given subexpression, leaving context-dependent fields to be filled later
- This is a critical function in PostgreSQL's query optimization process, as RestrictInfo nodes are fundamental data structures used throughout the planner

## Simplified Source

```c
RestrictInfo *
make_restrictinfo(PlannerInfo *root,
                  Expr *clause,
                  bool is_pushed_down,
                  bool has_clone,
                  bool is_clone,
                  bool pseudoconstant,
                  Index security_level,
                  Relids required_relids,
                  Relids incompatible_relids,
                  Relids outer_relids)
{
    // Handle OR clauses with special recursive processing
    if (is_orclause(clause))
        return (RestrictInfo *) make_sub_restrictinfos(root,
                                                       clause,
                                                       is_pushed_down,
                                                       has_clone,
                                                       is_clone,
                                                       pseudoconstant,
                                                       security_level,
                                                       required_relids,
                                                       incompatible_relids,
                                                       outer_relids);

    // AND clauses should have been flattened earlier
    Assert(!is_andclause(clause));

    // Create standard RestrictInfo for the clause
    return make_restrictinfo_internal(root,
                                      clause,
                                      NULL,
                                      is_pushed_down,
                                      has_clone,
                                      is_clone,
                                      pseudoconstant,
                                      security_level,
                                      required_relids,
                                      incompatible_relids,
                                      outer_relids);
}
```