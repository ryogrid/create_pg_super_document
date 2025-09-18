# add_eq_member

## Location
src/backend/optimizer/path/equivclass.c: 516 - 585

## Overview
Creates a new EquivalenceMember node and adds it to an existing EquivalenceClass, handling the classification of expressions as constants or relations-dependent members.

## Definition


## Detailed Description
This internal function constructs a new EquivalenceMember node and integrates it into an existing EquivalenceClass. It performs several important classifications and updates:

1. **Constant Detection**: If the expression has no relation dependencies (empty relids), it's classified as a pseudoconstant and the EC is marked as containing constants.

2. **Parent-Child Relationships**: Supports hierarchical member relationships through the parent parameter, where child members don't contribute to the EC's relation set.

3. **Relation Tracking**: Updates the EquivalenceClass's ec_relids bitmap to include relations referenced by non-child members.

The function assumes that expressions from process_equivalence() are already validated (no aggregates, SRFs, volatility checked), but places the burden of more thorough validation on callers like get_eclass_for_sort_expr().

## Parameters / Member Variables
- : EquivalenceClass to add the new member to
- : Expression to be represented by this member
- : Bitmap of relation IDs referenced by the expression
- : JoinDomain within which this member is valid
- : Parent EquivalenceMember for child relationships, NULL for top-level members
- : Data type of the expression

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - bms_is_empty
  - bms_add_members
  - lappend
- Called from (representative examples):
  - process_equivalence
  - get_eclass_for_sort_expr
  - add_child_rel_equivalences
  - add_setop_child_rel_equivalences

## Notes and Other Information
- Static function, only accessible within equivclass.c
- Child members (those with a non-NULL parent) don't contribute to the EquivalenceClass's ec_relids
- The function assumes expressions from process_equivalence() have already been validated for volatility and prohibited constructs
- Automatically sets em_is_const=true for expressions with no relation dependencies
- Returns the newly created EquivalenceMember for use by the caller