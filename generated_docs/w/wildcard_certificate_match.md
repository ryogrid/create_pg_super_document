# wildcard_certificate_match

## Location
[src/interfaces/libpq/fe-secure-common.c:45-86](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-common.c#L45-L86)

## Overview
Validates whether a wildcard certificate pattern matches a given hostname string according to RFC2818 guidelines with stricter security constraints.

## Definition

```c
static bool
wildcard_certificate_match(const char *pattern, const char *string)
```
## Detailed Description
This function implements wildcard matching for SSL/TLS certificate hostname verification in PostgreSQL's libpq client library. It follows a conservative interpretation of RFC2818 wildcard certificate matching rules, prioritizing security over compatibility with some browser implementations.

The function enforces four key matching rules:
1. Only the '*' character is recognized as a wildcard
2. Wildcards are only matched at the start of the pattern
3. The '*' character does not match '.', limiting matches to single pathname components
4. Only one '*' wildcard is supported per pattern

All matching is performed case-insensitively since DNS is inherently case-insensitive. The implementation uses  for case-insensitive string comparison and includes validation to prevent common wildcard certificate security vulnerabilities.

## Parameters / Member Variables
- : The wildcard pattern from the certificate (e.g., "*.example.com")
- : The hostname to match against the pattern

## Dependencies
- Functions called/Symbols referenced:
  - strlen (C standard library)
  - [pg_strcasecmp](../p/pg_strcasecmp.md) (PostgreSQL case-insensitive string comparison)
  - strchr (C standard library)
- Called from (representative examples):
  - [pq_verify_peer_name_matches_certificate_name](../p/pq_verify_peer_name_matches_certificate_name.md)

## Notes and Other Information
- The function is more restrictive than most browser implementations, particularly in rule 3 where '*' does not match '.' characters
- This conservative approach prevents certain classes of certificate validation attacks
- The pattern must start with '*.' to be considered a valid wildcard pattern
- The function performs length validation to ensure the pattern can theoretically match the string before attempting detailed comparison
- Returns false for any malformed or potentially unsafe wildcard patterns