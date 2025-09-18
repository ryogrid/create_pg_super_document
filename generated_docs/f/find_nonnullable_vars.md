# find_nonnullable_vars

## Location
[src/backend/optimizer/util/clauses.c:1707-1712](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L1707-L1712)

## Overview
Determines which Vars are forced nonnullable by a given clause, returning the set of variables that cannot be NULL for the clause to return TRUE.

## Definition
List *find_nonnullable_vars(Node *clause)

## Detailed Description
This function analyzes a Boolean expression to identify level-zero Vars that are referenced in such a way that the clause cannot possibly return TRUE if any of these Vars is NULL. The function serves as a wrapper that calls the walker function with top_level set to true.

The analysis is conservative and simplistic by design. The function assumes the input is a Boolean expression that has been AND/OR flattened and converted to implicit-AND format. The semantics differ from contain_nonstrict_functions in that this function specifically looks for NULL inputs that will provably cause a FALSE-or-NULL result in Boolean contexts.

The function returns variable information as a multibitmapset (List of Bitmapsets), where List indexes correspond to relation IDs (varnos), and the per-relation Bitmapsets hold variable attribute numbers offset by FirstLowInvalidHeapAttributeNumber.

## Parameters / Member Variables
- clause: The Boolean expression node to analyze for nonnullable variable constraints

## Dependencies
- Functions called/Symbols referenced:
  - [find_nonnullable_vars_walker](find_nonnullable_vars_walker.md)
- Called from (representative examples):
  - [reduce_outer_joins_pass2](../r/reduce_outer_joins_pass2.md)
  - WindowFuncLists

## Notes and Other Information
- The function is designed to err on the side of conservatism in its analysis
- It expects expressions to be in AND/OR flattened and implicit-AND format
- The returned multibitmapset structure efficiently represents variable constraints across multiple relations
- This function is part of PostgreSQL's query optimization infrastructure for handling outer join reduction and null-aware optimizations