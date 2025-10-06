# network_scan_first

## Location
[src/backend/utils/adt/network.c:1690-1703](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L1690-L1703)

## Overview
Returns the minimal (first) IP address value for a given network, used by the PostgreSQL query planner to generate index scan limits for network containment operators.

## Definition

```c
Datum
network_scan_first(Datum in)
```
## Detailed Description
This function is specifically designed to support the PostgreSQL query planner in generating efficient index scan limits for network containment queries using operators like << (network contains) and <<= (network contains or equals). It returns the network address itself, which represents the minimal possible IP address within that network range.

The function serves as a boundary function for index scans, helping the planner determine the lower bound when scanning network-type indexes for containment relationships. By returning the network address (the first address in the range), it enables efficient range scans on network indexes.

## Parameters / Member Variables
- `in`: Input Datum containing the network value for which to find the first IP address
## Dependencies
- Functions called/Symbols referenced:
  - DirectFunctionCall1 (PostgreSQL function call interface)
  - [network_network](network_network.md) (function that extracts network address from network type)

- Called from (representative examples):
  - [match_network_subset](../m/match_network_subset.md) (network subset matching function)

## Notes and Other Information
- This function is part of PostgreSQL's network operator optimization infrastructure
- Works in conjunction with network_scan_last to define complete scan ranges for network containment queries
- Simply delegates to network_network since the network address is already the minimal address in the range
- Essential for efficient execution of queries involving network containment operations on indexed columns
- The function returns a Datum, PostgreSQL's universal data value type

## Simplified Source

```c
Datum
network_scan_first(Datum in)
{
    // Return the network address itself (the first IP in the range)
    return DirectFunctionCall1(network_network, in);
}
```