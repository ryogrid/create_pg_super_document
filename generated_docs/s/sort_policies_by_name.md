# sort_policies_by_name

## Location
[src/backend/rewrite/rowsecurity.c:665-673](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rowsecurity.c#L665-L673)

## Overview
This static utility function sorts a list of row-level security policies by their names to ensure deterministic ordering of restrictive policy evaluation.

## Definition


## Detailed Description
The  function provides deterministic ordering for restrictive row-level security policies by sorting them alphabetically by name. This is crucial for restrictive policies because each policy generates separate WithCheckOption checks that must be applied in a consistent, predictable order. The function serves as a thin wrapper around the generic  function, using  as the comparison function.

This sorting is only applied to restrictive policies because they are combined using AND logic (all must pass), making the order of evaluation significant for consistent error reporting and performance. Permissive policies, in contrast, are combined using OR logic into a single check, making their individual order irrelevant.

## Parameters / Member Variables
- : A List of RowSecurityPolicy structures to be sorted in-place by policy name

## Dependencies
- Functions called/Symbols referenced:
  - [list_sort](../l/list_sort.md)
  - [row_security_policy_cmp](../r/row_security_policy_cmp.md)
- Called from (representative examples):
  - [get_policies_for_relation](../g/get_policies_for_relation.md) (for both built-in and hook-provided restrictive policies)

## Notes and Other Information
- Only used for restrictive policies, not permissive policies
- Ensures consistent ordering of WithCheckOption generation and evaluation
- The sorting is performed in-place, modifying the original list
- Critical for deterministic behavior in multi-policy scenarios
- Policy name comparison is case-sensitive and follows standard string collation rules