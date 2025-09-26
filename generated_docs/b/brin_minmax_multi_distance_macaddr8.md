# brin_minmax_multi_distance_macaddr8

## Location
[src/backend/access/brin/brin_minmax_multi.c:2249-2296](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L2249-L2296)

## Overview
Computes the distance between two 8-byte MAC address values by treating them as base-256 numbers and calculating their numerical difference, used by BRIN minmax multi operator classes for macaddr8 data types.

## Definition
```c
Datum brin_minmax_multi_distance_macaddr8(PG_FUNCTION_ARGS)
```

## Detailed Description
This function calculates the numerical distance between two 8-byte MAC address values (macaddr8) by interpreting each address as a base-256 number. The calculation processes each byte from the most significant (field 'h') to least significant (field 'a'), building up the difference using successive division by 256 to properly weight each byte position. This approach extends the algorithm used for standard 6-byte MAC addresses and UUIDs to handle the extended 64-bit MAC address format. The function is part of the BRIN minmax multi operator class infrastructure, enabling efficient indexing of macaddr8 columns.

## Parameters / Member Variables
- `PG_GETARG_MACADDR8_P(0)`: Pointer to the first 8-byte MAC address (a)
- `PG_GETARG_MACADDR8_P(1)`: Pointer to the second 8-byte MAC address (b)
- Returns: `float8` representing the numerical distance between the MAC addresses

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MACADDR8_P (macro for extracting macaddr8 pointer arguments)
  - PG_RETURN_FLOAT8 (macro for returning float8 result)  
  - [macaddr8](../m/macaddr8.md) (PostgreSQL 8-byte MAC address data type structure with fields a,b,c,d,e,f,g,h)
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- The function assumes the second MAC address (b) >= first MAC address (a) and includes an Assert to verify this
- MAC address fields are processed in reverse order (h,g,f,e,d,c,b,a) representing most to least significant bytes
- Each byte contributes to the final distance with appropriate base-256 weighting (division by 256 after each addition)
- The calculation treats 8-byte MAC addresses as 64-bit integers in base-256 representation
- This extends the algorithm used for standard 6-byte MAC addresses to handle EUI-64 format addresses
- This function is typically registered in BRIN operator class definitions for macaddr8 columns
- The distance represents the numerical gap between MAC addresses in the extended address space
- [macaddr8](../m/macaddr8.md) supports both EUI-64 and modified EUI-64 formats used in IPv6 link-local addresses