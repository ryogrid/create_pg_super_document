# inet_gist_consistent

## Location
src/backend/utils/adt/network_gist.c: 115 - 344

## Overview
The GiST query consistency check function for inet data types that determines whether a query condition can be satisfied by values represented by a GiST index key.

## Definition


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