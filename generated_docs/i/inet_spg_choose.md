# inet_spg_choose

## Location
[src/backend/utils/adt/network_spgist.c:68-164](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network_spgist.c#L68-L164)

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
  - [DatumGetInetPP](../D/DatumGetInetPP.md) (datum to inet conversion)
  - ip_family (extract address family)
  - ip_bits (extract prefix length)
  - ip_addr (extract address bits)
  - [bitncmp](../b/bitncmp.md) (bitwise comparison)
  - [bitncommon](../b/bitncommon.md) (find common prefix bits)
  - [inet_spg_node_number](inet_spg_node_number.md) (calculate node number)
  - [cidr_set_masklen_internal](../c/cidr_set_masklen_internal.md) (set network mask length)
  - [InetPGetDatum](../I/InetPGetDatum.md) (inet to datum conversion)
  - [spgChooseIn](../s/spgChooseIn.md)/spgChooseOut (SP-GiST structures)
- Called from (representative examples):
  - SP-GiST insertion process
  - Index tree navigation during updates

## Notes and Other Information
- Handles both IPv4 and IPv6 addresses through address family separation
- Uses bitwise operations for efficient prefix comparison and common bit calculation
- Creates 2-node tuples for address family splits and 4-node tuples for prefix-based splits
- The function ensures that addresses from different families never coexist under the same inner node
- Supports the allTheSame optimization where all values in a subtree are identical
- [Node](../N/Node.md) numbering follows a specific pattern based on address bits for consistent tree structure

## Simplified Source

```c
Datum
inet_spg_choose(PG_FUNCTION_ARGS)
{
    spgChooseIn *in = (spgChooseIn *) PG_GETARG_POINTER(0);
    spgChooseOut *out = (spgChooseOut *) PG_GETARG_POINTER(1);
    inet *val = DatumGetInetPP(in->datum);

    // Handle address family splits (IPv4 vs IPv6)
    if (!in->hasPrefix) {
        out->resultType = spgMatchNode;
        out->result.matchNode.nodeN = (ip_family(val) == PGSQL_AF_INET) ? 0 : 1;
        out->result.matchNode.restDatum = InetPGetDatum(val);
        PG_RETURN_VOID();
    }

    // Handle prefix-based navigation/splitting
    inet *prefix = DatumGetInetPP(in->prefixDatum);
    int commonbits = ip_bits(prefix);

    // Different family - create 2-node family split
    if (ip_family(val) != ip_family(prefix)) {
        out->resultType = spgSplitTuple;
        out->result.splitTuple.prefixHasPrefix = false;
        out->result.splitTuple.prefixNNodes = 2;
        out->result.splitTuple.childNodeN = (ip_family(prefix) == PGSQL_AF_INET) ? 0 : 1;
        out->result.splitTuple.postfixHasPrefix = true;
        out->result.splitTuple.postfixPrefixDatum = InetPGetDatum(prefix);
        PG_RETURN_VOID();
    }

    // Check if prefix split needed
    if (ip_bits(val) < commonbits ||
        bitncmp(ip_addr(prefix), ip_addr(val), commonbits) != 0) {
        // Create 4-node prefix split
        commonbits = bitncommon(ip_addr(prefix), ip_addr(val),
                                Min(ip_bits(val), commonbits));
        out->resultType = spgSplitTuple;
        out->result.splitTuple.prefixHasPrefix = true;
        out->result.splitTuple.prefixPrefixDatum =
            InetPGetDatum(cidr_set_masklen_internal(val, commonbits));
        out->result.splitTuple.prefixNNodes = 4;
        out->result.splitTuple.childNodeN = inet_spg_node_number(prefix, commonbits);
        out->result.splitTuple.postfixHasPrefix = true;
        out->result.splitTuple.postfixPrefixDatum = InetPGetDatum(prefix);
        PG_RETURN_VOID();
    }

    // Navigate to appropriate child node
    out->resultType = spgMatchNode;
    out->result.matchNode.nodeN = inet_spg_node_number(val, commonbits);
    out->result.matchNode.restDatum = InetPGetDatum(val);
    PG_RETURN_VOID();
}
```