# get_gating_quals

## Location
src/backend/optimizer/plan/createplan.c: 1003 - 1022

## Overview
Extracts pseudoconstant qualifiers from a node's quals list for gating purposes in query execution plans.

## Definition
```c
static List *get_gating_quals(PlannerInfo *root, List *quals)
```

## Detailed Description
The `get_gating_quals` function is a utility used during query plan creation to identify and extract pseudoconstant qualifiers from a list of query qualifications. Pseudoconstant qualifiers are conditions that can be evaluated early in query execution to potentially gate (short-circuit) the execution of more expensive operations. The function first checks if there are any pseudoconstant qualifiers present in the planner info, and if so, orders the qualifiers for optimal execution and extracts only the pseudoconstant ones.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global information about the query being planned, including the hasPseudoConstantQuals flag
- `quals`: List of RestrictInfo nodes representing the qualification conditions to be examined

## Dependencies
- Functions called/Symbols referenced:
  - order_qual_clauses
  - extract_actual_clauses
- Called from (representative examples):
  - create_scan_plan (multiple locations)
  - create_join_plan

## Notes and Other Information
- Returns NIL if no pseudoconstant quals are present (determined by root->hasPseudoConstantQuals flag)
- The function is static, meaning it's only used within the createplan.c file
- Pseudoconstant quals are important for query optimization as they can be evaluated once and used to gate further processing
- The extracted quals are returned in actual clause form (not RestrictInfo form) for execution