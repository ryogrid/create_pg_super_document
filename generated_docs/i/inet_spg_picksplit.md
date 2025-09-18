# inet_spg_picksplit

## Location
src/backend/utils/adt/network_spgist.c: 165 - 238

## Overview
SP-GiST picksplit function for inet/cidr data types that partitions a set of network address values into child nodes when a leaf page overflows.

## Definition
```c
Datum inet_spg_picksplit(PG_FUNCTION_ARGS)
```

## Detailed Description
The `inet_spg_picksplit` function is called when a leaf node in the SP-GiST index becomes too full and needs to be split into multiple child nodes. It analyzes a collection of network address values to determine the optimal way to partition them for efficient tree structure.

The function operates in two distinct modes:

1. **Address Family Split**: When the input values contain different address families (IPv4 and IPv6), it creates a 2-node structure where node 0 contains IPv4 addresses and node 1 contains IPv6 addresses.

2. **Prefix-based Split**: When all values belong to the same address family, it determines the longest common prefix among all values and creates a 4-node structure. Each node represents one of four possible bit patterns (00, 01, 10, 11) for the bit following the common prefix.

The function performs a two-pass algorithm: first examining all values to find the common characteristics (family differences and minimum common prefix length), then partitioning the values accordingly. It uses bitwise operations for efficient prefix comparison and employs helper functions to determine the appropriate node assignment.

## Parameters / Member Variables
- `in`: Input structure containing:
  - `datums`: Array of network address values to be split
  - `nTuples`: Number of values in the input array
- `out`: Output structure containing:
  - `hasPrefix`: Whether the resulting split has a prefix
  - `prefixDatum`: The common prefix for the split (if applicable)
  - `nNodes`: Number of child nodes created (2 or 4)
  - `nodeLabels`: Labels for nodes (always NULL for inet)
  - `mapTuplesToNodes`: Array mapping input values to output nodes
  - `leafTupleDatums`: Array of processed values for leaf storage

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInetPP](../D/DatumGetInetPP.md) (datum to inet conversion)
  - ip_family (extract address family)
  - ip_bits (extract prefix length)
  - ip_addr (extract address bits)
  - [bitncommon](../b/bitncommon.md) (find common prefix bits)
  - [cidr_set_masklen_internal](../c/cidr_set_masklen_internal.md) (set network mask length)
  - [inet_spg_node_number](inet_spg_node_number.md) (calculate node number)
  - [InetPGetDatum](../I/InetPGetDatum.md) (inet to datum conversion)
  - [palloc](../p/palloc.md) (memory allocation)
  - [spgPickSplitIn](../s/spgPickSplitIn.md)/spgPickSplitOut (SP-GiST structures)
- Called from (representative examples):
  - SP-GiST leaf node splitting process
  - Index maintenance during bulk insertions

## Notes and Other Information
- Always creates either 2-node (family-based) or 4-node (prefix-based) splits
- Uses bitwise prefix analysis to minimize the tree depth and maximize search efficiency
- Handles mixed IPv4/IPv6 scenarios by separating them at the address family level
- The 4-node split follows a quadtree-like pattern based on the next bit pair after the common prefix
- Memory allocation is handled through PostgreSQL's palloc mechanism
- [Node](../N/Node.md) labels are not used (set to NULL) since network address comparison is done directly on the data
- The function ensures balanced partitioning by using the longest possible common prefix