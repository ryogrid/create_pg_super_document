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

## Simplified Source

```c
Datum
inet_spg_inner_consistent(PG_FUNCTION_ARGS)
{
    spgInnerConsistentIn *in = (spgInnerConsistentIn *) PG_GETARG_POINTER(0);
    spgInnerConsistentOut *out = (spgInnerConsistentOut *) PG_GETARG_POINTER(1);
    int which;

    if (!in->hasPrefix) {
        // Address family split (2 nodes: IPv4, IPv6)
        which = 1 | (1 << 1);  // Start with both nodes

        for (int i = 0; i < in->nkeys; i++) {
            StrategyNumber strategy = in->scankeys[i].sk_strategy;
            inet *argument = DatumGetInetPP(in->scankeys[i].sk_argument);

            switch (strategy) {
                case RTLessStrategyNumber:
                case RTLessEqualStrategyNumber:
                    if (ip_family(argument) == PGSQL_AF_INET)
                        which &= 1;  // Only IPv4 node
                    break;
                case RTGreaterEqualStrategyNumber:
                case RTGreaterStrategyNumber:
                    if (ip_family(argument) == PGSQL_AF_INET6)
                        which &= (1 << 1);  // Only IPv6 node
                    break;
                case RTNotEqualStrategyNumber:
                    break;  // Keep both
                default:
                    // Family-specific operators
                    if (ip_family(argument) == PGSQL_AF_INET)
                        which &= 1;
                    else
                        which &= (1 << 1);
                    break;
            }
        }
    } else if (!in->allTheSame) {
        // Prefix-based split (4 nodes)
        which = inet_spg_consistent_bitmap(DatumGetInetPP(in->prefixDatum),
                                           in->nkeys, in->scankeys, false);
    } else {
        // All the same - must visit all nodes
        which = ~0;
    }

    // Build output node list
    out->nNodes = 0;
    if (which) {
        out->nodeNumbers = (int *) palloc(sizeof(int) * in->nNodes);
        for (int i = 0; i < in->nNodes; i++) {
            if (which & (1 << i)) {
                out->nodeNumbers[out->nNodes] = i;
                out->nNodes++;
            }
        }
    }

    PG_RETURN_VOID();
}
```