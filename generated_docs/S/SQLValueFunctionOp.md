# SQLValueFunctionOp

## Location
[src/include/nodes/primnodes.h:1551-1552](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L1551-L1552)

## Overview
SQLValueFunctionOp is an enumeration that defines the types of parameterless SQL functions with special grammar productions, categorized as datetime value functions and general value specifications in the SQL standard.

## Definition
```c
typedef enum SQLValueFunctionOp
{
    SVFOP_CURRENT_DATE,
    SVFOP_CURRENT_TIME,
    SVFOP_CURRENT_TIME_N,
    SVFOP_CURRENT_TIMESTAMP,
    SVFOP_CURRENT_TIMESTAMP_N,
    SVFOP_LOCALTIME,
    SVFOP_LOCALTIME_N,
    SVFOP_LOCALTIMESTAMP,
    SVFOP_LOCALTIMESTAMP_N,
    SVFOP_CURRENT_ROLE,
    SVFOP_CURRENT_USER,
    SVFOP_USER,
    SVFOP_SESSION_USER,
    SVFOP_CURRENT_CATALOG,
    SVFOP_CURRENT_SCHEMA,
} SQLValueFunctionOp;
```

## Detailed Description
SQLValueFunctionOp serves as a type identifier for parameterless SQL functions that require special grammar productions. These functions fall into two main categories as defined by the SQL standard: datetime value functions (like CURRENT_DATE, CURRENT_TIME, etc.) and general value specifications (like CURRENT_USER, SESSION_USER, etc.). The enumeration provides a way to distinguish between different function types without needing individual handling for each function. All variants return non-collating datatypes and are considered stable functions.

## Parameters / Member Variables
- `SVFOP_CURRENT_DATE`: Returns the current date
- `SVFOP_CURRENT_TIME`: Returns the current time without precision specification
- `SVFOP_CURRENT_TIME_N`: Returns the current time with precision specification
- `SVFOP_CURRENT_TIMESTAMP`: Returns the current timestamp without precision specification
- `SVFOP_CURRENT_TIMESTAMP_N`: Returns the current timestamp with precision specification
- `SVFOP_LOCALTIME`: Returns local time without precision specification
- `SVFOP_LOCALTIME_N`: Returns local time with precision specification
- `SVFOP_LOCALTIMESTAMP`: Returns local timestamp without precision specification
- `SVFOP_LOCALTIMESTAMP_N`: Returns local timestamp with precision specification
- `SVFOP_CURRENT_ROLE`: Returns the current role identifier
- `SVFOP_CURRENT_USER`: Returns the current user identifier
- `SVFOP_USER`: Returns the user identifier (synonym for CURRENT_USER)
- `SVFOP_SESSION_USER`: Returns the session user identifier
- `SVFOP_CURRENT_CATALOG`: Returns the current catalog name
- `SVFOP_CURRENT_SCHEMA`: Returns the current schema name

## Dependencies
- Functions called/Symbols referenced:
  - None (this is an enumeration)
- Called from (representative examples):
  - SQLValueFunction struct (used as op field type)

## Notes and Other Information
- All functions represented by this enumeration are parameterless and have special grammar productions
- All variants return non-collating datatypes, eliminating the need for a collation field
- All these functions are stable (their results don't change within a transaction)
- The enumeration is used in the SQLValueFunction struct to identify which specific function is being represented
- Type and typmod information is stored separately to avoid needing individual handling for each function type