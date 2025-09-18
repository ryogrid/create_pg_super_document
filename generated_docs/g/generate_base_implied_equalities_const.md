# generate_base_implied_equalities_const

## Location
src/backend/optimizer/path/equivclass.c: 1108 - 1202

## Overview
Generates implied equality clauses for equivalence classes that contain pseudoconstants by creating "member = const" restrictions for each non-constant member.

## Definition


## Detailed Description
This function handles the specific case where an EquivalenceClass contains one or more constant or pseudoconstant members. It implements an optimization strategy that generates equality clauses comparing each variable member to a chosen constant member, effectively constraining all variables at their points of creation without requiring variable-to-variable comparisons.

The function employs a preference hierarchy when selecting the constant member, favoring actual constants over pseudoconstants (such as Params) because constraint exclusion machinery can better optimize "var = const" equalities compared to "var = param" expressions.

For the trivial case of exactly two members with one source clause (a simple "var = const"), the function optimizes by reusing the original clause rather than rebuilding an equivalent one.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state information
- : EquivalenceClass containing constant members to process

## Dependencies
- Functions called/Symbols referenced:
  - [distribute_restrictinfo_to_rels](../d/distribute_restrictinfo_to_rels.md)
  - [select_equality_operator](../s/select_equality_operator.md)
  - [process_implied_equality](../p/process_implied_equality.md)
- Called from (representative examples):
  - [generate_base_implied_equalities](generate_base_implied_equalities.md)

## Notes and Other Information
- Prefers actual Const nodes over other pseudoconstants for better constraint exclusion
- Handles the trivial two-member, one-source case by reusing the original RestrictInfo
- Uses the constant's em_jdomain as qualscope for generated clauses
- Marks the EC as broken (ec_broken = true) if required equality operators are not available
- Generated clauses are stored in ec_derives for potential selectivity estimation use
- Sets mergejoinable clause markings (left_ec, right_ec, left_em, right_em) for non-degenerate clauses
- Does not generate join clauses since ec_has_const eclasses are not used for joins
- Located in src/backend/optimizer/path/equivclass.c:1108-1202