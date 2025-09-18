# network_overlap

## Location
src/backend/utils/adt/network.c: 963 - 980

## Overview
Implements the network overlap operator (&&) for INET data types, checking if two networks have any overlapping address space.

## Definition
```c
Datum network_overlap(PG_FUNCTION_ARGS)
```

## Detailed Description
The `network_overlap` function implements PostgreSQL's network overlap operator (&&) for INET data types. It determines whether two network arguments have any overlapping address space by checking if they share a common network prefix.

The function first verifies that both networks belong to the same IP family (IPv4 or IPv6). If they do, it performs the overlap check by comparing the network portions of both addresses using bitwise comparison for the length of the shorter prefix (minimum of both networks' prefix lengths).

Two networks overlap if their network addresses are identical when compared up to the shorter of their two prefix lengths. This means that one network contains the other, or they share a common address range.

If the networks belong to different IP families, the function returns false as cross-family overlap is not meaningful.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `a1`: First INET network value
  - `a2`: Second INET network value

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INET_PP (extract INET arguments)
  - ip_family (get IP family of network)
  - bitncmp (bitwise comparison of network addresses)
  - ip_addr (get network address)
  - ip_bits (get prefix length of network)
  - Min (get minimum of two values)
  - PG_RETURN_BOOL (return boolean result)
- Called from (representative examples):
  - No direct references found (likely called via SQL operator &&)

## Notes and Other Information
- This function implements the && operator in SQL for INET types
- Returns true if the networks have any overlapping address space
- Cross-family comparisons (IPv4 vs IPv6) always return false
- Uses the minimum prefix length of both networks to determine the comparison scope
- Two networks overlap if they share the same network address up to the shorter prefix length
- Used internally by PostgreSQL's network operator system for INET overlap operations