# inet_spg_consistent_bitmap

## Location
[src/backend/utils/adt/network_spgist.c:374-712](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network_spgist.c#L374-L712)

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

## Simplified Source

```c
static int inet_spg_consistent_bitmap(const inet *prefix, int nkeys, ScanKey scankeys, bool leaf) {
    int bitmap;
    int commonbits;

    // Initialize bitmap: leaf returns 0/1, inner returns bits for 4 children
    if (leaf)
        bitmap = 1;
    else
        bitmap = 1 | (1 << 1) | (1 << 2) | (1 << 3);  // All 4 children

    commonbits = ip_bits(prefix);

    // Check each scan key condition
    for (int i = 0; i < nkeys; i++) {
        inet *argument = DatumGetInetPP(scankeys[i].sk_argument);
        StrategyNumber strategy = scankeys[i].sk_strategy;

        // Check 1: Address family compatibility
        if (ip_family(argument) != ip_family(prefix)) {
            // Handle family mismatch for comparison strategies
            switch (strategy) {
                case RTLessStrategyNumber:
                case RTLessEqualStrategyNumber:
                    if (ip_family(argument) < ip_family(prefix))
                        bitmap = 0;
                    break;
                case RTGreaterEqualStrategyNumber:
                case RTGreaterStrategyNumber:
                    if (ip_family(argument) > ip_family(prefix))
                        bitmap = 0;
                    break;
                default:
                    if (strategy != RTNotEqualStrategyNumber)
                        bitmap = 0;
                    break;
            }
            if (!bitmap) break;
            continue;  // Skip other checks for different families
        }

        // Check 2: Network bit count for subnet/supernet operations
        switch (strategy) {
            case RTSubStrategyNumber:
                if (commonbits <= ip_bits(argument))
                    bitmap &= (1 << 2) | (1 << 3);  // Higher branches only
                break;
            case RTSuperStrategyNumber:
                if (commonbits >= ip_bits(argument))
                    bitmap = 0;
                else if (commonbits == ip_bits(argument) - 1)
                    bitmap &= 1 | (1 << 1);  // Lower branches only
                break;
            case RTEqualStrategyNumber:
                if (commonbits < ip_bits(argument))
                    bitmap &= (1 << 2) | (1 << 3);
                else if (commonbits == ip_bits(argument))
                    bitmap &= 1 | (1 << 1);
                else
                    bitmap = 0;
                break;
        }

        if (!bitmap) break;

        // Check 3: Compare common address bits
        int order = bitncmp(ip_addr(prefix), ip_addr(argument),
                           Min(commonbits, ip_bits(argument)));

        if (order != 0) {
            // Handle address mismatch for comparison strategies
            switch (strategy) {
                case RTLessStrategyNumber:
                case RTLessEqualStrategyNumber:
                    if (order > 0) bitmap = 0;
                    break;
                case RTGreaterEqualStrategyNumber:
                case RTGreaterStrategyNumber:
                    if (order < 0) bitmap = 0;
                    break;
                default:
                    if (strategy != RTNotEqualStrategyNumber)
                        bitmap = 0;
                    break;
            }
            if (!bitmap) break;
            continue;
        }

        // Check 4: Next bit optimization for performance
        if (bitmap & ((1 << 2) | (1 << 3)) && commonbits < ip_bits(argument)) {
            int nextbit = ip_addr(argument)[commonbits / 8] &
                         (1 << (7 - commonbits % 8));

            // Filter branches based on next bit value
            switch (strategy) {
                case RTLessStrategyNumber:
                case RTLessEqualStrategyNumber:
                    if (!nextbit)
                        bitmap &= 1 | (1 << 1) | (1 << 2);
                    break;
                case RTGreaterEqualStrategyNumber:
                case RTGreaterStrategyNumber:
                    if (nextbit)
                        bitmap &= 1 | (1 << 1) | (1 << 3);
                    break;
                default:
                    if (strategy != RTNotEqualStrategyNumber) {
                        if (!nextbit)
                            bitmap &= 1 | (1 << 1) | (1 << 2);
                        else
                            bitmap &= 1 | (1 << 1) | (1 << 3);
                    }
                    break;
            }
            if (!bitmap) break;
        }

        // Check 5: Final address comparison for leaf nodes
        if (leaf) {
            order = bitncmp(ip_addr(prefix), ip_addr(argument),
                           ip_maxbits(prefix));

            switch (strategy) {
                case RTLessStrategyNumber:
                    if (order >= 0) bitmap = 0;
                    break;
                case RTLessEqualStrategyNumber:
                    if (order > 0) bitmap = 0;
                    break;
                case RTEqualStrategyNumber:
                    if (order != 0) bitmap = 0;
                    break;
                case RTGreaterEqualStrategyNumber:
                    if (order < 0) bitmap = 0;
                    break;
                case RTGreaterStrategyNumber:
                    if (order <= 0) bitmap = 0;
                    break;
                case RTNotEqualStrategyNumber:
                    if (order == 0) bitmap = 0;
                    break;
            }
            if (!bitmap) break;
        }
    }

    return bitmap;
}
```