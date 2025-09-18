# get_proposed_default_constraint

## Location
[src/backend/catalog/partition.c:370-392](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/partition.c#L370-L392)

## Overview
Generates the constraint that would apply to a default partition after adding a new partition, by negating the new partition's constraints and putting the result in canonical form.

## Definition
```c
List *get_proposed_default_constraint(List *new_part_constraints)
```

## Detailed Description
This function computes what the default partition's constraints would become after adding a new partition to a partitioned table. Since the default partition must accept all rows that don't match any other partition, its constraints are the negation of the union of all other partitions' constraints.

The function works by:
1. Converting the input constraints to explicit AND form using make_ands_explicit()
2. Creating a NOT expression that negates the entire constraint set
3. Simplifying the negated expression using constant evaluation
4. Canonicalizing the result to put it in standard form
5. Converting back to implicit AND form for the final result

This is particularly important when adding new partitions, as it allows the system to determine if the existing default partition data would violate the new constraint regime.

## Parameters / Member Variables
- `new_part_constraints`: List of constraint expressions from the new partition being added

## Dependencies
- Functions called/Symbols referenced:
  - [make_ands_explicit](../m/make_ands_explicit.md)
  - [makeBoolExpr](../m/makeBoolExpr.md)
  - NOT_EXPR (constant)
  - [eval_const_expressions](../e/eval_const_expressions.md)
  - [canonicalize_qual](../c/canonicalize_qual.md)
  - [make_ands_implicit](../m/make_ands_implicit.md)
- Called from (representative examples):
  - [ATExecAttachPartition](../A/ATExecAttachPartition.md)
  - [check_default_partition_contents](../c/check_default_partition_contents.md)

## Notes and Other Information
- The function assumes partition constraints never evaluate to NULL, making the negation operation safe
- Uses eval_const_expressions() with NULL context to simplify the negated expression
- The canonicalize_qual() call with true parameter ensures the result is in canonical form
- The result represents the constraint that would need to be satisfied by all rows in the default partition after the new partition is added
- This is essential for validating that existing default partition data remains consistent when new partitions are added