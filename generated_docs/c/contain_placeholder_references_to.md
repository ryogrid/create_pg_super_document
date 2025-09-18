# contain_placeholder_references_to

## Location
[src/backend/optimizer/util/placeholder.c:464-478](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/placeholder.c#L464-L478)

## Overview
Detects whether any PlaceHolderVars in a given clause contain references to a specified relation ID, typically used for outer join analysis.

## Definition
```c
bool contain_placeholder_references_to(PlannerInfo *root, Node *clause, int relid)
```

## Detailed Description
This function examines a clause (expression tree) to determine if any PlaceHolderVars within it contain references to a specific relation ID. This is particularly important for outer join processing, where changing the nullability status of a relation might affect what PlaceHolderVars compute. The function provides a quick optimization by checking if any PlaceHolderVars exist at all before performing the recursive search.

The term "contain" means that there's a use of the specified relid inside the PHV's contained expression, indicating that nullability changes to that relation could affect the PHV's computation. The function uses a walker pattern to recursively traverse the expression tree.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planning information
- `clause`: Expression tree (Node) to search for placeholder references
- `relid`: Relation ID to search for within placeholder expressions

## Dependencies
- Functions called/Symbols referenced:
  - [contain_placeholder_references_context](contain_placeholder_references_context.md) (context structure for walker)
  - [contain_placeholder_references_walker](contain_placeholder_references_walker.md) (recursive walker function)
- Called from (representative examples):
  - [make_outerjoininfo](../m/make_outerjoininfo.md) (src/backend/optimizer/plan/initsplan.c:1512)

## Notes and Other Information
- The function includes an optimization that returns false immediately if no PlaceHolderVars exist in the query (lastPHId == 0)
- The code includes handling for upper-level PHVs which is noted as likely dead code but kept for safety
- This function is typically used during outer join analysis to determine if placeholder variables might be affected by join nullability
- The function initializes a context structure with the target relid and sublevels_up counter before calling the walker
- Returns true if any PlaceHolderVar in the clause references the specified relation ID