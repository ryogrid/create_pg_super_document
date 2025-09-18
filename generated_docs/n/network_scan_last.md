# network_scan_last

## Location
src/backend/utils/adt/network.c: 1704 - 1715

## Overview
Returns the maximal (last) IP address value for a given network by computing the broadcast address with maximum mask length, used by the query planner for generating index scan upper bounds.

## Definition


## Detailed Description
This function calculates the highest possible IP address within a network range for use by the PostgreSQL query planner in generating efficient index scan limits. It works by first obtaining the broadcast address of the network (the highest address in the range) and then setting the mask length to maximum (32 for IPv4, 128 for IPv6).

The function is crucial for network containment queries because it ensures proper ordering comparisons. For example, 192.168.0.255/24 should be considered less than 192.168.0.255/32 in network ordering. By maximizing the mask length, the function ensures that broadcast addresses are compared correctly in index scans.

The implementation leverages two key operations:
1. Calculate the broadcast address using network_broadcast
2. Set the mask length to maximum (-1 triggers max mask length behavior in inet_set_masklen)

## Parameters / Member Variables
- : Input Datum containing the network value for which to find the last IP address

## Dependencies
- Functions called/Symbols referenced:
  - DirectFunctionCall2 (PostgreSQL function call interface for 2-argument functions)
  - DirectFunctionCall1 (PostgreSQL function call interface for 1-argument functions)
  - inet_set_masklen (function to set network mask length, -1 sets to maximum)
  - network_broadcast (function to compute broadcast address of a network)
  - Int32GetDatum (converts integer to Datum type)

- Called from (representative examples):
  - match_network_subset (network subset matching function)

## Notes and Other Information
- Works as the upper bound complement to network_scan_first for index scan range definition
- The special value -1 passed to inet_set_masklen triggers maximum mask length behavior (32 for IPv4, 128 for IPv6)
- Essential for proper network address ordering in index operations
- Ensures that network containment queries can be efficiently executed using index range scans
- Part of PostgreSQL's network operator optimization infrastructure