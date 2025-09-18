# inet_spg_node_number

## Location
src/backend/utils/adt/network_spgist.c: 350 - 373

## Overview
Calculates the node number within a 4-node, single-family inner index tuple for SP-GiST indexing of network addresses.

## Definition


## Detailed Description
This static function determines which of the 4 possible child nodes a given network address value should be routed to within an SP-GiST inner index tuple. The function uses a 2-bit encoding scheme where:
- The least significant bit (bit 0) indicates whether the next address bit after commonbits is set (even/odd selection)
- Bit 1 indicates whether the value's mask length is greater than commonbits (low/high node selection)

The function assumes the value has the same address family as the node's prefix and uses commonbits as the mask length of the prefix to determine the routing decision.

## Parameters / Member Variables
- `val`: Pointer to the inet value being indexed
- `commonbits`: The mask length of the prefix (number of common bits in the node's prefix)

## Dependencies
- Functions called/Symbols referenced:
  - ip_maxbits: Gets the maximum number of bits for the address family
  - ip_addr: Gets the address bytes of the inet value
  - ip_bits: Gets the mask length (network bits) of the inet value
- Called from (representative examples):
  - inet_spg_choose: Uses this function to determine which child node to route values to during index construction
  - inet_spg_picksplit: Uses this function during node splitting operations

## Notes and Other Information
- This is a static helper function specific to network address SP-GiST indexing
- The 4-node organization allows efficient routing based on both the next address bit and mask length comparison
- The function implements a key part of the SP-GiST quad-tree structure for network addresses
- Performance-critical code path for network address indexing and searching operations