# inet_spg_inner_consistent

## Location
[src/backend/utils/adt/network_spgist.c:239-322](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network_spgist.c#L239-L322)

## Overview
SP-GiST inner consistency function for inet/cidr data types that determines which child nodes need to be visited during index searches based on query predicates.

## Definition
```c
Datum inet_spg_inner_consistent(PG_FUNCTION_ARGS)
```

## Detailed Description
The `inet_spg_inner_consistent` function is a crucial component of SP-GiST query processing that evaluates whether child nodes of an inner tuple need to be visited during a search operation. It analyzes the search keys (query conditions) against the structure of the current inner node to determine which child nodes might contain matching data.

The function operates in three distinct scenarios based on the node structure:

1. **Address Family Nodes** (no prefix, 2 nodes): When dealing with a node that splits by address family, it evaluates which address families (IPv4/IPv6) need to be searched based on the query operators and arguments. For example, less-than operations on IPv4 addresses only need to search the IPv4 subtree.

2. **Prefix-based Nodes** (with prefix, 4 nodes): For nodes that split based on network prefixes, it uses the `inet_spg_consistent_bitmap` helper function to determine which of the four child nodes (representing different bit patterns) might contain matching addresses.

3. **All-the-same Nodes**: When all values in a subtree are identical, it must visit all child nodes since the specific distribution is unknown.

The function uses a bitmask approach where each bit represents whether the corresponding child node should be visited, providing an efficient way to prune unnecessary subtree traversals during search operations.

## Parameters / Member Variables
- `in`: Input structure containing:
  - `hasPrefix`: Whether this inner node has a prefix
  - `prefixDatum`: The prefix value (if hasPrefix is true)
  - `nNodes`: Number of child nodes
  - `nkeys`: Number of search conditions
  - `scankeys`: Array of search key conditions with strategy and argument
  - `allTheSame`: Whether all values in subtree are identical
- `out`: Output structure containing:
  - `nNodes`: Number of child nodes to visit
  - `nodeNumbers`: Array of child node indices to visit

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInetPP](../D/DatumGetInetPP.md) (datum to inet conversion)
  - ip_family (extract address family)
  - [inet_spg_consistent_bitmap](inet_spg_consistent_bitmap.md) (evaluate prefix-based consistency)
  - [palloc](../p/palloc.md) (memory allocation)
  - Strategy constants (RTLessStrategyNumber, RTGreaterStrategyNumber, etc.)
  - [spgInnerConsistentIn](../s/spgInnerConsistentIn.md)/spgInnerConsistentOut (SP-GiST structures)
- Called from (representative examples):
  - SP-GiST query processing engine
  - Index search operations for network address queries

## Notes and Other Information
- Uses bitwise operations for efficient node selection through bitmask manipulation
- Handles IPv4/IPv6 address family separation by constraining searches to appropriate subtrees
- For inequality operators, applies address family-specific logic (IPv4 addresses are "less than" IPv6 addresses)
- The NOT EQUAL strategy is handled specially as it may require visiting multiple subtrees
- Supports the allTheSame optimization by visiting all nodes when specific distribution is unknown
- Memory allocation for output node numbers is done only when nodes need to be visited
- The function assumes less than 32 child nodes for efficient bitmask operations
- Strategy-based pruning significantly reduces the number of nodes visited during range queries