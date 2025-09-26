# sslVerifyProtocolRange

## Location
[src/interfaces/libpq/fe-connect.c:7614-7666](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L7614-L7666)

## Overview
Validates that the SSL protocol version range (minimum and maximum) is logically correct and consistent.

## Definition
```c
static bool sslVerifyProtocolRange(const char *min, const char *max)
```

## Detailed Description
This function ensures that the SSL protocol range specified by minimum and maximum version parameters is valid and logically consistent. It performs TLS backend-agnostic validation by operating on string representations of the protocol versions. The function expects that both input parameters have already been validated using sslVerifyProtocolVersion(). It handles various edge cases including unset bounds and ensures the minimum version is not greater than the maximum version.

## Parameters / Member Variables
- `min`: A string specifying the minimum SSL/TLS protocol version (can be NULL or empty)
- `max`: A string specifying the maximum SSL/TLS protocol version (can be NULL or empty)

## Dependencies
- Functions called/Symbols referenced:
  - [sslVerifyProtocolVersion](sslVerifyProtocolVersion.md) (validation of individual protocol versions)
  - strlen (standard C library function)
  - [pg_strcasecmp](../p/pg_strcasecmp.md) (PostgreSQL case-insensitive string comparison)
  - Assert (PostgreSQL assertion macro)
- Called from (representative examples):
  - internalPQconninfoOption
  - [pqConnectOptions2](../p/pqConnectOptions2.md)

## Notes and Other Information
- [Range](../R/Range.md) is valid if at least one bound is unset (NULL or empty string)
- If minimum is "TLSv1" (lowest supported), any maximum is valid
- Maximum cannot be "TLSv1" if minimum is a higher version
- Uses string comparison to ensure min ≤ max for TLSv1.1 through TLSv1.3
- Static function scope limited to fe-connect.c
- Includes assertion to verify inputs are pre-validated