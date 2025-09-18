# QualItem

## Location
src/backend/optimizer/plan/createplan.c: 5323 - 5409

## Overview
A local structure used within the order_qual_clauses function to temporarily hold qualification clauses along with their costs and security levels for sorting purposes.

## Definition
```c
typedef struct
{
    Node       *clause;
    Cost        cost;
    Index       security_level;
} QualItem;
```

## Detailed Description
QualItem is a temporary data structure used exclusively within the order_qual_clauses function to facilitate the sorting of qualification clauses. The structure bundles together a clause node with its associated execution cost and security level, enabling efficient comparison and ordering during the sorting process.

The function creates an array of QualItem structures to avoid repeated cost evaluations, then sorts them using insertion sort (chosen over qsort for stability). Special handling is applied for leakproof clauses that are relatively cheap (less than 10X cpu_operator_cost) - these are assigned security_level 0 to allow them to be positioned earlier in the execution order, potentially improving performance by filtering out rows sooner.

After sorting, the clauses are extracted back into a list in their optimal execution order, balancing both cost and security considerations.

## Parameters / Member Variables
- `clause`: Pointer to the actual qualification clause (Node)
- `cost`: Per-tuple execution cost of the clause (Cost type)  
- `security_level`: Security level constraint for clause placement (Index type)

## Dependencies
- Functions called/Symbols referenced:
  - [cost_qual_eval_node](../c/cost_qual_eval_node.md)
  - cpu_operator_cost (global variable)
- Called from (representative examples):
  - [order_qual_clauses](../o/order_qual_clauses.md) (internal usage only)

## Notes and Other Information
- This is a local typedef within the order_qual_clauses function, not globally visible
- Used only for temporary storage during the clause ordering optimization process
- The sorting algorithm prioritizes security_level first, then cost within the same security level
- Leakproof clauses get special treatment to potentially improve query performance while maintaining security guarantees
- The structure is allocated on the stack using palloc and deallocated automatically when the function returns