# match_network_subset

## Location
[src/backend/utils/adt/network.c:1076-1172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L1076-L1172)

## Overview
Generates index qualification conditions for network subset operations by creating range constraints using network_scan_first and network_scan_last functions.

## Definition
```c
static List *match_network_subset(Node *leftop, Node *rightop, bool is_eq, Oid opfamily)
```

## Detailed Description
The `match_network_subset` function is the core logic for converting network subset operations into index-scannable conditions. It transforms a network subset query into a pair of range conditions that can be efficiently executed using B-tree indexes.

The function works by:

1. **Validation**: Ensures the right operand is a non-null constant and the operator family is the expected network B-tree family
2. **Range Generation**: Uses `network_scan_first` and `network_scan_last` to compute the range of network addresses that would match the subset condition
3. **Clause Construction**: Creates two comparison clauses:
   - Lower bound: "key >= network_scan_first(rightopval)" (or ">" for strict subset)
   - Upper bound: "key <= network_scan_last(rightopval)"

This approach converts a complex network containment test into simple range comparisons that can leverage existing B-tree index infrastructure.

For example, a query like "WHERE network_col <<= '192.168.1.0/24'" becomes:
- "WHERE network_col >= '192.168.1.0/32' AND network_col <= '192.168.1.255/32'"

## Parameters / Member Variables
- `leftop`: Left operand node (typically the indexed column)
- `rightop`: Right operand node (must be a constant network value)  
- `is_eq`: Boolean indicating if equality is allowed (true for <<=, false for <<)
- `opfamily`: Operator family OID (must be NETWORK_BTREE_FAM_OID)

## Dependencies
- Functions called/Symbols referenced:
  - IsA (type checking macro for Const nodes)
  - [get_opfamily_member](../g/get_opfamily_member.md) (lookup operators by strategy number)
  - [network_scan_first](../n/network_scan_first.md) (get first address in network range)
  - [network_scan_last](../n/network_scan_last.md) (get last address in network range)
  - make_opclause (create operator expression nodes)
  - [makeConst](makeConst.md) (create constant expression nodes)
  - list_make1, lappend (list construction functions)
  - elog (error logging)
- Called from (representative examples):
  - [match_network_function](match_network_function.md) (network function dispatcher)

## Notes and Other Information
- This is a static helper function, not directly callable from outside the module
- Only works with constant right operands - variable comparisons cannot be optimized this way
- Requires the specific network B-tree operator family (NETWORK_BTREE_FAM_OID)
- The `is_eq` parameter determines whether to use >= or > for the lower bound
- Both clauses use the same left operand but different constant right operands
- The generated range constraints allow efficient index scans instead of full table scans
- Returns NIL (empty list) if optimization is not possible
- Part of PostgreSQL's advanced index optimization infrastructure for network data types
- Strategy numbers (BTGreaterEqualStrategyNumber, etc.) correspond to standard B-tree comparison operators