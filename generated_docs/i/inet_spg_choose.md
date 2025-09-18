# inet_spg_choose

## Location
src/backend/utils/adt/network_spgist.c: 68 - 164

## Overview
SP-GiST choose function for inet/cidr data types that determines how to navigate or split the index tree when inserting new network address values.

## Definition
```c
Datum inet_spg_choose(PG_FUNCTION_ARGS)
```

## Detailed Description
The `inet_spg_choose` function is a critical component of the SP-GiST indexing mechanism for network addresses. It makes decisions about how to handle new data insertions into the index tree. The function operates in three main scenarios:

1. **Address Family Routing**: When encountering a tuple that splits by address family (IPv4 vs IPv6), it routes the value to the appropriate subnet (node 0 for IPv4, node 1 for IPv6).

2. **Family-based Splitting**: When a new value belongs to a different address family than the existing prefix, it creates a new 2-node tuple to separate IPv4 and IPv6 addresses.

3. **Prefix-based Operations**: For values within the same address family, it either:
   - Navigates to the appropriate child node if the value matches the existing prefix
   - Splits the tuple into a 4-node structure when the new value requires a different prefix length

The function uses bit-level comparison to determine common prefixes and employs helper functions like `inet_spg_node_number` to calculate the appropriate node for insertion.

## Parameters / Member Variables
- `in`: Input structure containing:
  - `datum`: The network address value being inserted
  - `hasPrefix`: Whether the current node has a prefix
  - `prefixDatum`: The prefix value of the current node
  - `nNodes`: Number of child nodes
  - `allTheSame`: Whether all values in this subtree are identical
- `out`: Output structure for returning the decision result

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetInetPP (datum to inet conversion)
  - ip_family (extract address family)
  - ip_bits (extract prefix length)
  - ip_addr (extract address bits)
  - bitncmp (bitwise comparison)
  - bitncommon (find common prefix bits)
  - inet_spg_node_number (calculate node number)
  - cidr_set_masklen_internal (set network mask length)
  - InetPGetDatum (inet to datum conversion)
  - spgChooseIn/spgChooseOut (SP-GiST structures)
- Called from (representative examples):
  - SP-GiST insertion process
  - Index tree navigation during updates

## Notes and Other Information
- Handles both IPv4 and IPv6 addresses through address family separation
- Uses bitwise operations for efficient prefix comparison and common bit calculation
- Creates 2-node tuples for address family splits and 4-node tuples for prefix-based splits
- The function ensures that addresses from different families never coexist under the same inner node
- Supports the allTheSame optimization where all values in a subtree are identical
- Node numbering follows a specific pattern based on address bits for consistent tree structure