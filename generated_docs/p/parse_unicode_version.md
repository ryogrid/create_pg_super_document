# parse_unicode_version

## Location
[src/common/unicode/category_test.c:35-55](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode/category_test.c#L35-L55)

## Overview
Parses a Unicode version string into an integer format for easy numerical comparison during testing.

## Definition

```c
static int
parse_unicode_version(const char *version)
```
## Detailed Description
This static function converts a Unicode version string (in the format "major.minor") into a single integer value that enables straightforward numerical comparisons. The function uses  to extract the major and minor version numbers from the input string and combines them using the formula: . This encoding assumes that minor version numbers are always less than 100, which is validated through an assertion.

The function is specifically designed for use in PostgreSQL's Unicode category testing framework, where different Unicode version behaviors need to be compared and handled appropriately.

## Parameters / Member Variables
- `*version`: A null-terminated string containing the Unicode version in "major.minor" format (e.g., "13.0", "15.1")
## Dependencies
- Functions called/Symbols referenced:
  -  (standard C library function)
  -  (PostgreSQL assertion macro)
  -  (PostgreSQL macro for variables only used in assertions)

- Called from (representative examples):
  -  (in category_test.c at lines 225 and 229)

## Notes and Other Information
- The function is marked as , making it internal to the category_test.c file
- Uses assertions to validate that exactly 2 values are parsed and that the minor version is less than 100
- The encoding scheme allows for major versions up to 2147483647 and minor versions up to 99
- This function is part of PostgreSQL's Unicode testing infrastructure and is not intended for general-purpose use
- The PG_USED_FOR_ASSERTS_ONLY attribute on variable n prevents compiler warnings about unused variables in release builds where assertions are disabled

## Simplified Source

```c
static int parse_unicode_version(const char *version) {
    int n PG_USED_FOR_ASSERTS_ONLY;
    int major;
    int minor;

    // Parse version string "major.minor" format
    n = sscanf(version, "%d.%d", &major, &minor);

    Assert(n == 2);        // Must parse exactly 2 numbers
    Assert(minor < 100);   // Minor version must be < 100

    // Encode as single integer: major * 100 + minor
    return major * 100 + minor;
}
```