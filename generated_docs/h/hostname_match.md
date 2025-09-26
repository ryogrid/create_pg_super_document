# hostname_match

## Location
src/backend/libpq/hba.c: 1052 - 1071

## Overview
Compares a hostname pattern against an actual hostname, supporting both exact matches and suffix-based domain matching.

## Definition

```c
struct addrinfo *gai_result,
			   *gai;
```
## Detailed Description
The  function performs hostname pattern matching for PostgreSQL's Host-Based Authentication (HBA) system. It supports two types of matching:

1. **Suffix matching**: When the pattern starts with a dot (e.g., ".example.com"), it matches any hostname that ends with that suffix. This allows matching entire domains and subdomains.

2. **Exact matching**: When the pattern doesn't start with a dot, it performs a case-insensitive exact match against the entire hostname.

For suffix matching, the function calculates the lengths of both strings and ensures the actual hostname is at least as long as the pattern. It then compares the suffix of the actual hostname with the pattern. All comparisons are case-insensitive using PostgreSQL's  function.

## Parameters / Member Variables
- : The hostname pattern to match against (may start with '.' for suffix matching)
- : The actual hostname to be checked against the pattern

## Dependencies
- Functions called/Symbols referenced:
  - strlen (standard C library function)
  - pg_strcasecmp (PostgreSQL's case-insensitive string comparison)
- Called from (representative examples):
  - check_hostname (in hba.c)

## Notes and Other Information
- Supports domain suffix matching when pattern starts with '.' (e.g., ".example.com" matches "host.example.com")
- Uses case-insensitive comparison for all hostname matching
- Part of PostgreSQL's HBA hostname-based authentication system
- Efficient implementation that avoids unnecessary string operations
- Does not support wildcards or regular expressions (only exact and suffix matching)
- Assumes both input strings are null-terminated C strings