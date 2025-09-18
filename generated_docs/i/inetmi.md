# inetmi

## Location
[src/backend/utils/adt/network.c:2018-2094](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L2018-L2094)

## Overview
A PostgreSQL built-in function that computes the difference between two inet addresses, returning a 64-bit signed integer representing the numerical distance between them.

## Definition
Datum inetmi(PG_FUNCTION_ARGS)

## Detailed Description
The `inetmi` function implements inet address subtraction by computing the numerical difference between two inet addresses of the same family (IPv4 or IPv6). It uses two's complement arithmetic to perform the subtraction, treating IP addresses as large binary integers and computing their difference byte by byte.

The function employs the traditional complement-increment-add approach: it complements the bits of the second address, adds 1 (handled by initializing carry to 1), and adds the result to the first address. This effectively computes `ip1 - ip2`.

The implementation includes comprehensive overflow detection for cases where the result exceeds the range of a 64-bit signed integer, and proper sign extension for addresses narrower than 64 bits.

## Parameters / Member Variables
- Function uses PostgreSQL's `PG_FUNCTION_ARGS` convention:
  - Argument 0: First inet address (accessed via `PG_GETARG_INET_PP`)
  - Argument 1: Second inet address to subtract (accessed via `PG_GETARG_INET_PP`)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INET_PP
  - ip_family
  - ip_addrsize
  - ip_addr
  - ereport
  - PG_RETURN_INT64
- Called from (representative examples):
  - SQL queries using inet - inet operator

## Notes and Other Information
- Requires both inet addresses to be of the same family (IPv4 or IPv6)
- Uses two's complement arithmetic for robust subtraction across different address widths
- Handles overflow detection for results that exceed int64 range
- Performs proper sign extension for addresses smaller than 64 bits
- Supports the SQL `-` operator for inet - inet operations
- Returns a signed 64-bit integer that can be positive or negative depending on address ordering
- Part of PostgreSQL's network data type arithmetic infrastructure
- More complex than integer subtraction due to multi-byte address handling and overflow checking