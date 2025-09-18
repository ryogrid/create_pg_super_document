# inet_spg_consistent_bitmap

## Location
src/backend/utils/adt/network_spgist.c: 374 - 712

## Overview
Calculates a bitmap of node numbers that are consistent with query conditions for SP-GiST indexing of network addresses, supporting both inner node traversal and leaf node validation.

## Definition
```c
static int inet_spg_consistent_bitmap(const inet *prefix, int nkeys, ScanKey scankeys, bool leaf)
```

## Detailed Description
This comprehensive function performs consistency checking for network address queries in SP-GiST indexes. It evaluates multiple scan keys against a given prefix to determine which child nodes should be visited (for inner nodes) or whether a leaf value matches (for leaf nodes). The function implements a sophisticated 6-step checking process:

1. **Family Check**: Verifies address family compatibility
2. **Network Bit Count Check**: Compares network mask lengths for subnet operations
3. **Common Network Bits Check**: Compares address prefixes up to the minimum mask length
4. **Next Network Bit Check**: Uses the next bit after common prefix for performance optimization
5. **Network Bit Count Re-check**: Re-evaluates mask lengths for basic comparison strategies
6. **Whole Address Check**: Performs complete address comparison for leaf nodes

For inner nodes, the function returns a 4-bit bitmap indicating which children to visit. For leaf nodes, it returns 0 (no match) or 1 (match).

## Parameters / Member Variables
- `prefix`: The inet prefix value stored at the current node
- `nkeys`: Number of scan key conditions to evaluate
- `scankeys`: Array of scan key conditions from the query
- `leaf`: Boolean indicating whether this is a leaf node consistency check

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInetPP](../D/DatumGetInetPP.md): Extracts inet pointer from Datum
  - ip_family: Gets address family (IPv4/IPv6)
  - ip_bits: Gets network mask length
  - ip_addr: Gets address bytes
  - ip_maxbits: Gets maximum bits for address family
  - [bitncmp](../b/bitncmp.md): Performs bit-wise comparison of addresses
  - Various RT strategy constants (RTLessStrategyNumber, RTEqualStrategyNumber, etc.)
- Called from (representative examples):
  - [inet_spg_inner_consistent](inet_spg_inner_consistent.md): Uses this for inner node consistency checking
  - [inet_spg_leaf_consistent](inet_spg_leaf_consistent.md): Uses this for leaf node matching

## Notes and Other Information
- Supports all network address comparison strategies: \<, \<=, =, \>=, \>, \<\>, subnet (\<\<=), supernet (\>\>=), etc.
- Implements performance optimizations by eliminating impossible paths early
- Handles both IPv4 and IPv6 addresses with family-aware comparisons  
- Critical for query performance in network address SP-GiST indexes
- The dual inner/leaf functionality reduces code duplication while maintaining efficiency
- Uses bit manipulation techniques for efficient address comparison at the bit level