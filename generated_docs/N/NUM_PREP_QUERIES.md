# NUM_PREP_QUERIES

## Location
src/bin/pg_dump/pg_backup.h: 77 - 80

## Overview
A sentinel constant that defines the total count of prepared queries used by pg_dump, serving as the upper bound for the prepared query enumeration.

## Definition
```c
enum _dumpPreparedQueries
{
    PREPQUERY_DUMPAGG,
    PREPQUERY_DUMPBASETYPE,
    PREPQUERY_DUMPCOMPOSITETYPE,
    PREPQUERY_DUMPDOMAIN,
    PREPQUERY_DUMPENUMTYPE,
    PREPQUERY_DUMPFUNC,
    PREPQUERY_DUMPOPR,
    PREPQUERY_DUMPRANGETYPE,
    PREPQUERY_DUMPTABLEATTACH,
    PREPQUERY_GETCOLUMNACLS,
    PREPQUERY_GETDOMAINCONSTRAINTS,
    NUM_PREP_QUERIES			/* must be last */
};
```

## Detailed Description
`NUM_PREP_QUERIES` is a special enumeration value that automatically represents the total number of prepared queries defined in the `_dumpPreparedQueries` enum. By being placed as the last item in the enum, it provides a compile-time constant equal to the count of all preceding enum values, which is essential for array sizing and loop bounds in pg_dump's prepared statement management.

## Parameters / Member Variables
- This is not a function or struct, but an enum constant with automatic value assignment based on its position

## Dependencies
- Functions called/Symbols referenced:
  - Part of the `_dumpPreparedQueries` enumeration
- Called from (representative examples):
  - `setup_connection` function in pg_dump.c for array allocation and prepared statement initialization

## Notes and Other Information
This constant is crucial for pg_dump's performance optimization through prepared statements. It ensures that the prepared statement arrays are correctly sized to accommodate all defined queries. The comment "must be last" is a critical maintenance note - adding new prepared queries requires inserting them before NUM_PREP_QUERIES, not after. This pattern is a common C idiom for maintaining counts of enumerated items automatically.