# inet_gist_consistent

## Location
[src/backend/utils/adt/network_gist.c:115-344](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network_gist.c#L115-L344)

## Overview
The GiST query consistency check function for inet data types that determines whether a query condition can be satisfied by values represented by a GiST index key.

## Definition

```c
Datum
inet_gist_consistent(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the GiST consistent method for inet/cidr data types, performing multi-level consistency checking to determine if a GiST index entry can contain values that satisfy a given query condition. The function performs five sequential checks:

1. **Different families check**: Handles cases where the key represents multiple address families (IPv4/IPv6)
2. **Family mismatch check**: Compares address families between key and query for ordering strategies
3. **Network bit count check**: Validates network mask bit counts for subnet/supernet operations
4. **Common network bits check**: Compares the network portion of addresses up to the minimum common bits
5. **Whole address check**: Final comparison of complete addresses when network portions match

The function supports all inet comparison strategies including equality, ordering (lt/le/ge/gt), containment (sub/subeq/sup/supeq), overlap, and inequality operations. All operators are marked as exact (no recheck required).

## Parameters / Member Variables
- : GiST entry containing the index key to check
- : The inet value being searched for
- : The comparison strategy number (INETSTRAT_*)
- : Output parameter indicating if recheck is needed (always set to false)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER, PG_GETARG_INET_PP, PG_GETARG_UINT16
  - DatumGetInetKeyP
  - gk_ip_family, gk_ip_minbits, gk_ip_commonbits, gk_ip_maxbits, gk_ip_addr
  - ip_family, ip_bits, ip_addr
  - GIST_LEAF
  - [bitncmp](../b/bitncmp.md)
  - Min
  - INETSTRAT_* constants
- Called from (representative examples):
  - GiST index access methods (indirectly through function pointer)

## Notes and Other Information
- All operators served by this function are exact, eliminating the need for tuple rechecking
- The function handles both leaf and internal index pages differently
- For internal pages with mixed address families (family = 0), the function conservatively returns true
- The implementation mirrors the logic in network_cmp_internal() for consistent ordering
- Error handling includes an assertion for unknown strategies with elog(ERROR)

## Simplified Source

```c
Datum inet_gist_consistent(PG_FUNCTION_ARGS) {
    GISTENTRY *entry = (GISTENTRY *) PG_GETARG_POINTER(0);
    inet *query = PG_GETARG_INET_PP(1);
    StrategyNumber strategy = (StrategyNumber) PG_GETARG_UINT16(2);
    bool *recheck = (bool *) PG_GETARG_POINTER(4);
    GistInetKey *key = DatumGetInetKeyP(entry->key);

    // All operators served by this function are exact
    *recheck = false;

    // Check 1: Handle mixed address families in internal nodes
    if (gk_ip_family(key) == 0) {
        return PG_RETURN_BOOL(true);  // Mixed families - could match anything
    }

    // Check 2: Different address families between key and query
    if (gk_ip_family(key) != ip_family(query)) {
        switch (strategy) {
            case INETSTRAT_LT:
            case INETSTRAT_LE:
                return PG_RETURN_BOOL(gk_ip_family(key) < ip_family(query));
            case INETSTRAT_GE:
            case INETSTRAT_GT:
                return PG_RETURN_BOOL(gk_ip_family(key) > ip_family(query));
            case INETSTRAT_NE:
                return PG_RETURN_BOOL(true);
            default:
                return PG_RETURN_BOOL(false);
        }
    }

    // Check 3: Network bit count validation for subnet operations
    switch (strategy) {
        case INETSTRAT_SUB:
            if (GIST_LEAF(entry) && gk_ip_minbits(key) <= ip_bits(query))
                return PG_RETURN_BOOL(false);
            break;
        case INETSTRAT_SUBEQ:
            if (GIST_LEAF(entry) && gk_ip_minbits(key) < ip_bits(query))
                return PG_RETURN_BOOL(false);
            break;
        case INETSTRAT_SUPEQ:
        case INETSTRAT_EQ:
            if (gk_ip_minbits(key) > ip_bits(query))
                return PG_RETURN_BOOL(false);
            break;
        case INETSTRAT_SUP:
            if (gk_ip_minbits(key) >= ip_bits(query))
                return PG_RETURN_BOOL(false);
            break;
    }

    // Check 4: Compare network portions
    int common_bits = Min(gk_ip_commonbits(key), gk_ip_minbits(key));
    common_bits = Min(common_bits, ip_bits(query));
    int network_comparison = bitncmp(gk_ip_addr(key), ip_addr(query), common_bits);

    // Handle network-only comparison strategies
    switch (strategy) {
        case INETSTRAT_SUB:
        case INETSTRAT_SUBEQ:
        case INETSTRAT_OVERLAPS:
        case INETSTRAT_SUPEQ:
        case INETSTRAT_SUP:
            return PG_RETURN_BOOL(network_comparison == 0);
    }

    // Continue with ordering strategies if at leaf level
    if (GIST_LEAF(entry)) {
        // Compare netmask bit counts
        switch (strategy) {
            case INETSTRAT_LT:
            case INETSTRAT_LE:
                if (gk_ip_minbits(key) != ip_bits(query))
                    return PG_RETURN_BOOL(gk_ip_minbits(key) < ip_bits(query));
                break;
            case INETSTRAT_EQ:
                if (gk_ip_minbits(key) != ip_bits(query))
                    return PG_RETURN_BOOL(false);
                break;
            case INETSTRAT_GE:
            case INETSTRAT_GT:
                if (gk_ip_minbits(key) != ip_bits(query))
                    return PG_RETURN_BOOL(gk_ip_minbits(key) > ip_bits(query));
                break;
            case INETSTRAT_NE:
                if (gk_ip_minbits(key) != ip_bits(query))
                    return PG_RETURN_BOOL(true);
                break;
        }

        // Final comparison of complete addresses
        int address_comparison = bitncmp(gk_ip_addr(key), ip_addr(query), gk_ip_maxbits(key));
        switch (strategy) {
            case INETSTRAT_LT:  return PG_RETURN_BOOL(address_comparison < 0);
            case INETSTRAT_LE:  return PG_RETURN_BOOL(address_comparison <= 0);
            case INETSTRAT_EQ:  return PG_RETURN_BOOL(address_comparison == 0);
            case INETSTRAT_GE:  return PG_RETURN_BOOL(address_comparison >= 0);
            case INETSTRAT_GT:  return PG_RETURN_BOOL(address_comparison > 0);
            case INETSTRAT_NE:  return PG_RETURN_BOOL(address_comparison != 0);
        }
    }

    // For internal nodes with matching network portions, descend
    return PG_RETURN_BOOL(network_comparison == 0);
}
```