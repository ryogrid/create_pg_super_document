# gmtload

## Location
[src/timezone/localtime.c:1245-1258](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/localtime.c#L1245-L1258)

## Overview
Loads the GMT (Greenwich Mean Time) timezone data into a timezone state structure, serving as a fallback mechanism for GMT timezone initialization.

## Definition

```c
static void
gmtload(struct state *const sp)
```
## Detailed Description
The  function is a static helper function that initializes a timezone state structure with GMT (Greenwich Mean Time) data. It attempts to load GMT timezone data using the standard  function first. If that fails (returns non-zero), it falls back to parsing a GMT timezone specification using . This two-step approach ensures that GMT timezone information is always available, even when timezone files are not accessible or corrupted.

The function operates as a last-resort mechanism for GMT timezone initialization, which is crucial for PostgreSQL's timezone handling system since GMT serves as a fundamental reference timezone.

## Parameters / Member Variables
- : Pointer to a  that will be populated with GMT timezone information. This structure contains timezone transition data, abbreviations, and other timezone-specific information.

## Dependencies
- Functions called/Symbols referenced:
  -  (attempts to load GMT timezone from file)
  -  (fallback parser for GMT timezone specification)
  -  (constant string "GMT")
- Called from (representative examples):
  -  (in src/timezone/localtime.c:1371)

## Notes and Other Information
- This is a static function, only accessible within the localtime.c file
- The  variable referenced is a constant string defined as "GMT" at line 53 of the same file
- The function guarantees that the state structure will be initialized with GMT data, providing robustness in timezone handling
- Part of PostgreSQL's timezone subsystem that handles conversion between different time representations
- The function passes  as the  parameter to  and the  parameter to , indicating extended format support and fallback parsing respectively