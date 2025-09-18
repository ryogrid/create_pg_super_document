# jpdsDateTimeNonZoned

## Location
src/backend/utils/adt/jsonpath.c: 1252 - 1255

## Overview
jpdsDateTimeNonZoned is an enumeration value within JsonPathDatatypeStatus enum that represents timezone-unaware datetime types in PostgreSQL's JSON path expressions.

## Definition
```c
enum JsonPathDatatypeStatus
{
    jpdsNonDateTime,        /* null, bool, numeric, string, array, object */
    jpdsUnknownDateTime,    /* unknown datetime type */
    jpdsDateTimeZoned,      /* timetz, timestamptz */
    jpdsDateTimeNonZoned,   /* time, timestamp, date */
};
```

## Detailed Description
jpdsDateTimeNonZoned is used within PostgreSQL's JSON path implementation to classify data types that represent datetime values without timezone information. This enum value specifically identifies time, timestamp, and date data types that do not include timezone components. It is primarily used in the JSON path mutability analysis system to determine whether a JSON path expression could potentially modify or depend on mutable datetime operations.

The enum serves as a status indicator in the JSON path walker functions that analyze the mutability characteristics of JSON path expressions, helping the query planner determine whether expressions contain mutable datetime functions that could affect query optimization and caching strategies.

## Parameters / Member Variables
This is an enumeration constant with no parameters or member variables.

## Dependencies
- Functions called/Symbols referenced: None (enumeration constant)
- Called from (representative examples):
  - jspIsMutableWalker (src/backend/utils/adt/jsonpath.c:1350, 1457, 1503, 1509)

## Notes and Other Information
- Part of the JsonPathDatatypeStatus enum used for JSON path mutability analysis
- Specifically represents timezone-unaware datetime types: DATEOID, TIMEOID, TIMESTAMPOID
- Used in the JSON path planner integration to determine expression mutability
- The enum value is assigned when processing datetime format operations and datetime constructor functions in JSON path expressions
- Helps distinguish between timezone-aware (jpdsDateTimeZoned) and timezone-unaware datetime operations
- Critical for proper handling of datetime operations in JSON path expressions within PostgreSQL's query planning and execution system