# pg_strncasecmp

## Location
[src/port/pgstrcasecmp.c:69-104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pgstrcasecmp.c#L69-L104)

## Overview
Performs case-independent comparison of two strings that are not necessarily null-terminated, examining at most n bytes from each string.

## Definition

```c
int
pg_strncasecmp(const char *s1, const char *s2, size_t n)
```
## Detailed Description
The  function compares two strings character by character in a case-insensitive manner, but with a length limit. It examines at most  bytes from each string, making it safe to use with non-null-terminated strings or when only comparing a prefix of strings. Like , it handles both ASCII characters (A-Z) with direct case conversion and locale-specific extended characters using standard C library functions.

The function decrements the counter  with each character comparison. If characters differ after case conversion, it returns the difference immediately. The comparison stops early if a null terminator is encountered in either string, even if the byte limit hasn't been reached. This makes it suitable for both null-terminated and fixed-length string comparisons.

## Parameters / Member Variables
- : First string to compare (not necessarily null-terminated)
- : Second string to compare (not necessarily null-terminated)
- : Maximum number of bytes to examine from each string

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to check if character has high bit set)
  - isupper (standard C library function for locale-aware uppercase detection)
  - tolower (standard C library function for locale-aware case conversion)
- Called from (representative examples):
  - [Boolean](../B/Boolean.md) value parsing (parse_bool_with_len)
  - Floating point number parsing (float4in_internal, float8in_internal)
  - [Numeric](../N/Numeric.md) input parsing (numeric_in)
  - Date/time style checking (check_datestyle)
  - psql command processing and tab completion
  - Configuration parameter validation

## Notes and Other Information
- Returns negative value if s1 < s2, positive if s1 > s2, zero if equal
- Stops comparison at the first null character encountered, even if n bytes haven't been examined
- Extensively used for parsing configuration values, SQL literals, and command-line options
- Critical for input validation where string length is limited or known in advance
- Provides safe string comparison for potentially unterminated character arrays