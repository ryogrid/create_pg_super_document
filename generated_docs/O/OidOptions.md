# OidOptions

## Location
src/bin/pg_dump/pg_dump.c: 101 - 164

## Overview
OidOptions is an enumeration used in pg_dump to control how zero (invalid) OIDs are handled when formatting type names and other object identifiers in SQL output.

## Definition


## Detailed Description
OidOptions provides a set of bit flags that control the behavior when encountering zero OIDs (which represent invalid or non-existent objects) during the formatting of SQL statements in pg_dump. This enumeration allows functions to specify how they want zero OIDs to be handled, providing flexibility in generating appropriate SQL output for different contexts.

The enumeration uses bit flags (powers of 2), allowing multiple options to be combined using bitwise OR operations if needed. The primary use case is in the getFormattedTypeName function, which formats PostgreSQL type names for inclusion in dump output.

## Parameters / Member Variables
- : Indicates that a zero OID should be treated as an error condition (value = 1)
- : When encountering a zero OID, output "*" as a placeholder (value = 2)
- : When encountering a zero OID, output "NONE" as a placeholder (value = 4)

## Dependencies
- Functions called/Symbols referenced:
  - Used as parameter type in function declarations
- Called from (representative examples):
  - [getFormattedTypeName](../g/getFormattedTypeName.md) (main function that uses these options to handle zero OIDs)
  - fmtQualifiedDumpable (function prototype that accepts OidOptions parameter)

## Notes and Other Information
- Used exclusively in pg_dump utility for controlling SQL output formatting
- The enumeration values are powers of 2, suggesting they were designed to be used as bit flags
- Most common usage is with zeroIsError for strict type checking, and zeroAsNone for contexts where missing types should be represented as "NONE"
- The zeroAsStar option is used in specific contexts where "*" is the appropriate placeholder for missing type information
- This pattern provides a clean way to handle the common case where database OIDs might be zero (invalid) and need to be represented appropriately in generated SQL