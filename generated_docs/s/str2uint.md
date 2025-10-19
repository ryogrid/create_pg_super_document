# str2uint

## Location
[src/bin/pg_upgrade/util.c:352-355](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/util.c#L352-L355)

## Overview
Converts a string representation of a number to an unsigned integer, specifically used in pg_upgrade utility for parsing configuration data.

## Definition

```c
unsigned int
str2uint(const char *str)
```
## Detailed Description
The  function is a utility function in the pg_upgrade tool that converts a null-terminated string containing a decimal number into an unsigned integer. It serves as a wrapper around the standard C library function , configured for base-10 conversion. The function is primarily used for parsing numeric values from PostgreSQL control data files during the database upgrade process.

The function performs no error checking - it relies entirely on the underlying  implementation. This means that invalid input strings may result in undefined behavior or return 0.

## Parameters / Member Variables
- `*str`: A null-terminated string containing the decimal representation of an unsigned integer to be converted
## Dependencies
- Functions called/Symbols referenced:
  - strtoul (standard C library function)
- Called from (representative examples):
  - [get_control_data](../g/get_control_data.md) (used extensively for parsing various control data fields like catalog version, control version, WAL block size, etc.)

## Notes and Other Information
- Located in src/bin/pg_upgrade/util.c:352-355
- The function comment incorrectly states "convert string to oid" but the function actually converts to unsigned int
- Used extensively in get_control_data() function to parse numeric fields from pg_controldata output
- No error checking is performed - invalid strings may cause undefined behavior
- The function assumes base-10 input (decimal numbers only)
- Part of the pg_upgrade utility which handles PostgreSQL major version upgrades

## Simplified Source

```c
unsigned int str2uint(const char *str) {
    // Convert decimal string to unsigned integer
    // Used for parsing numeric values from control data files
    return strtoul(str, NULL, 10);
}
```