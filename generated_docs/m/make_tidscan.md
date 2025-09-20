# make_tidscan

## Location
[src/backend/optimizer/plan/createplan.c:5646-5664](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L5646-L5664)

## Overview
Creates and initializes a TidScan plan node that directly accesses specific heap tuples using their tuple identifiers (TIDs), providing the most direct form of tuple access in PostgreSQL.

## Definition

```c
static TidScan *
make_tidscan(List *qptlist,
			 List *qpqual,
			 Index scanrelid,
			 List *tidquals)
```
## Detailed Description
This function constructs a TidScan plan node, which implements a highly specialized scan operation that directly fetches tuples from a heap table using their physical tuple identifiers (TIDs). TID scans are typically used when the query contains explicit CTID conditions or when the planner can determine that specific tuple locations need to be accessed directly. This is the most efficient way to access known tuples since it bypasses all indexing mechanisms and goes directly to the heap page and tuple offset specified by the TID.

## Parameters / Member Variables
- : Target list of expressions to be computed and returned by this scan
- : Additional qualification conditions to be evaluated against retrieved tuples
- : Range table index of the heap relation being scanned
- : List of qualification conditions that specify the TID values to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create the TidScan node)
- Called from (representative examples):
  - [create_tidscan_plan](../c/create_tidscan_plan.md)

## Notes and Other Information
- This is a static function within createplan.c for internal plan construction
- TID scans are most commonly triggered by queries with explicit CTID conditions
- Provides the most direct tuple access possible, bypassing all index structures
- The tidquals parameter contains conditions that evaluate to specific TID values
- No child plan nodes are needed since TID scans access tuples directly based on their physical addresses
- Very efficient for accessing a small number of known tuple locations