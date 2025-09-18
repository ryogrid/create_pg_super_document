# match_network_function

## Location
src/backend/utils/adt/network.c: 1028 - 1075

## Overview
Identifies network subset/superset functions and generates index qualification conditions by routing to the appropriate matching logic based on function ID.

## Definition
```c
static List *match_network_function(Node *leftop, Node *rightop, int indexarg, Oid funcid, Oid opfamily)
```

## Detailed Description
The `match_network_function` is a static helper function that serves as a dispatcher for generating index qualification conditions for network subset/superset functions. It analyzes the specific function being called and determines the appropriate optimization strategy.

The function examines the `funcid` parameter to identify which network operation is being performed:

- `F_NETWORK_SUB` (<<): subnet operator - indexkey must be on the left
- `F_NETWORK_SUBEQ` (<<=): subnet-or-equal operator - indexkey must be on the left  
- `F_NETWORK_SUP` (>>): supernet operator - indexkey must be on the right
- `F_NETWORK_SUPEQ` (>>=): supernet-or-equal operator - indexkey must be on the right

For each recognized function, it validates that the indexed argument is in the correct position (left or right operand), then delegates to `match_network_subset` with appropriately swapped arguments and the correct equality flag.

This design allows the same underlying matching logic to handle both subset and superset operations by swapping operand positions as needed.

## Parameters / Member Variables
- `leftop`: Left operand node of the network operation
- `rightop`: Right operand node of the network operation  
- `indexarg`: Position of the indexed argument (0 for left, 1 for right)
- `funcid`: OID of the network function being optimized
- `opfamily`: Operator family OID for the index

## Dependencies
- Functions called/Symbols referenced:
  - match_network_subset (core matching logic for network subset operations)
  - NIL (empty list constant)
- Called from (representative examples):
  - network_subset_support (planner support function)

## Notes and Other Information
- This is a static helper function, not directly callable from outside the module
- Acts as a dispatch layer that identifies function types and normalizes arguments
- Validates that the indexed column is in the correct position for each operation type
- For subset operations (<<, <<=), the indexed column must be the left operand
- For supernet operations (>>, >>=), the indexed column must be the right operand
- Returns NIL (empty list) if the function is unrecognized or arguments are in wrong positions
- The equality parameter passed to `match_network_subset` distinguishes between strict and non-strict containment
- Part of PostgreSQL's index optimization infrastructure for network data types