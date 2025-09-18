# add_base_clause_to_rel

## Location
src/backend/optimizer/plan/initsplan.c: 2629 - 2703

## Overview
Adds a RestrictInfo as a base restriction to a relation, with optimizations to handle trivially true/false qualifications and special inheritance table considerations.

## Definition
```c
static void add_base_clause_to_rel(PlannerInfo *root, Index relid,
                                  RestrictInfo *restrictinfo)
```

## Detailed Description
This function adds a qualification clause to a base relation's restriction list (baserestrictinfo), but includes several important optimizations and special case handling:

1. **Trivial Qualification Detection**: Checks if the qualification is always true or always false, applying optimizations accordingly
2. **Inheritance Table Handling**: Special logic for inheritance parent tables to ensure proper qual propagation to child tables
3. **Partitioned Table Optimization**: Applies constant folding optimizations to partitioned tables while preserving quals for child planning
4. **Security Level Management**: Updates the relation's minimum security level based on the added restriction
5. **Serial Number Preservation**: Maintains consistent serial numbers when transforming qualifications

The function performs these optimizations while preserving the semantic correctness required for inheritance and partitioning scenarios.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state
- `relid`: Index identifying the target base relation
- `restrictinfo`: RestrictInfo node containing the qualification clause to add

## Dependencies
- Functions called/Symbols referenced:
  - [find_base_rel](../f/find_base_rel.md)
  - [bms_membership](../b/bms_membership.md)
  - BMS_SINGLETON
  - [restriction_is_always_true](../r/restriction_is_always_true.md)
  - [restriction_is_always_false](../r/restriction_is_always_false.md)
  - [make_restrictinfo](../m/make_restrictinfo.md)
  - [makeBoolConst](../m/makeBoolConst.md)
- Called from (representative examples):
  - [distribute_restrictinfo_to_rels](../d/distribute_restrictinfo_to_rels.md)

## Notes and Other Information
Key aspects of the function's behavior:

**Inheritance Handling**: For inheritance parent tables (inh==true), the function generally preserves original RestrictInfo nodes to ensure apply_child_basequals() has access to the unmodified qualifications. However, partitioned tables are exempt from this restriction.

**Constant Folding**: When a qualification is proven to be always false, it's replaced with a constant FALSE expression while preserving the original rinfo_serial number to maintain consistency across related RestrictInfo nodes.

**Security Integration**: The function updates the relation's baserestrict_min_security to track the minimum security level among all base restrictions, which is important for row-level security enforcement.

**Assertion Requirements**: The function assumes the RestrictInfo references exactly one relation (BMS_SINGLETON membership), which is validated by assertion.

This function is essential for building the final restriction lists that drive relation scanning and filtering in the execution phase of query processing.