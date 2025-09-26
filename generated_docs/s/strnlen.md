# strnlen

## Location
src/port/strnlen.c: 26 - 33

## Overview
A portable implementation of the POSIX strnlen() function for systems where it's not natively available, providing safe string length calculation with a maximum boundary.

## Definition


## Detailed Description
The  function is a fallback implementation of the POSIX strnlen() function, designed to provide consistent behavior across different platforms where the native function might not be available. It safely calculates the length of a C-style null-terminated string up to a specified maximum length, preventing buffer overruns that could occur with the standard  function.

The function scans through the string character by character, counting each non-null character until either:
1. A null terminator ('\0') is encountered, or 
2. The maximum length () is reached

This bounded approach makes it particularly useful in security-sensitive contexts where input validation is critical, as it prevents reading beyond allocated memory boundaries when dealing with potentially unterminated strings.

Located in , this implementation is conditionally compiled and used only on systems that lack native strnlen support, as determined by the  configuration macro in .

## Parameters / Member Variables
- : Pointer to the null-terminated string whose length is to be calculated
- : Maximum number of characters to examine, providing an upper bound for safety

## Dependencies
- Functions called/Symbols referenced:
  - (None - uses only basic pointer arithmetic and dereferencing)
- Called from (representative examples):
  - parse_xml_decl (src/backend/utils/adt/xml.c:1471)
  - pnstrdup (src/backend/utils/mmgr/mcxt.c:1710, src/common/fe_memutils.c:157)
  - pg_encoding_mblen_bounded (src/common/wchar.c:2169)
  - PQescapeStringInternal (src/interfaces/libpq/fe-exec.c:4078)
  - PQescapeInternal (src/interfaces/libpq/fe-exec.c:4221)
  - PQmblenBounded (src/interfaces/libpq/fe-misc.c:1244)
  - fmtstr (src/port/snprintf.c:975)

## Notes and Other Information
- This function is part of PostgreSQL's portability layer, ensuring consistent behavior across different operating systems
- The implementation follows POSIX semantics: returns the actual string length if a null terminator is found within  characters, otherwise returns 
- Declared conditionally in  based on the  configuration macro
- Used extensively throughout PostgreSQL for safe string operations, particularly in memory allocation routines (), encoding validation, and SQL string escaping functions
- The function provides O(n) time complexity where n is the minimum of actual string length and 
- Critical for preventing buffer overflow vulnerabilities when processing untrusted input strings