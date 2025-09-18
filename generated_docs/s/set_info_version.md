# set_info_version

## Location
[src/bin/initdb/initdb.c:1927-1953](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L1927-L1953)

## Overview
Extracts and formats PostgreSQL version information into a specific format required for the information schema.

## Definition


## Detailed Description
The set_info_version function parses the PostgreSQL version string (PG_VERSION) and converts it into a standardized format required by the information schema specification. The function transforms a standard PostgreSQL version string into the format "XX.YY.ZZZZABC" where:

- XX is the major version (zero-padded to 2 digits)
- YY is the minor version (zero-padded to 2 digits)  
- ZZZZ is the micro/patch version (zero-padded to 4 digits)
- ABC represents any trailing alphabetic characters (like "devel", "beta", etc.)

The function handles version parsing by:
1. Creating a working copy of the PG_VERSION string
2. Locating the end of numeric characters to separate version numbers from trailing letters
3. Parsing major, minor, and micro version components using strtol()
4. Formatting the result into the global infoversion buffer

This formatted version is used by the information schema views to report PostgreSQL version information in a standardized SQL standard format.

## Parameters / Member Variables
This function takes no parameters and operates on global variables:
- Uses PG_VERSION constant as input
- Populates global infoversion buffer as output

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strdup](../p/pg_strdup.md) (PostgreSQL string duplication function)
  - strlen (standard C library function)
  - strtol (standard C library function for string to long conversion)
  - snprintf (standard C library function for formatted string output)
  - PG_VERSION (compile-time constant containing PostgreSQL version)

- Called from:
  - [main](../m/main.md) (initdb main function during initialization)

## Notes and Other Information
- The function allocates memory using pg_strdup() but does not explicitly free it, relying on process termination cleanup
- The "strange version" comment in the code refers to the specific formatting requirements of SQL information schema standards
- The function is designed to be called once during initdb execution to set up version information for the new database cluster
- The formatted version string is stored in a global buffer and used when creating information schema views