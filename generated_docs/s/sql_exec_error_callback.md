# sql_exec_error_callback

## Location
[src/backend/executor/functions.c:1406-1487](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/functions.c#L1406-L1487)

## Overview
An error context callback function that provides enhanced error reporting for SQL functions by adding call-stack traceback information and identifying the specific statement or phase where an error occurred.

## Definition


## Detailed Description
sql_exec_error_callback serves as PostgreSQL's error context callback specifically for SQL function execution. When an error occurs during SQL function execution, this callback enhances the error report by identifying the specific query statement within the function where the error occurred, handling syntax error position mapping from external to internal coordinates, and providing contextual information about the function name and execution phase. It traverses the function's execution states to pinpoint which statement was being executed when the error occurred, making debugging SQL functions significantly easier for developers.

## Parameters / Member Variables
- : A void pointer that is cast to FmgrInfo*, from which the SQLFunctionCache can be retrieved to access function execution context

## Dependencies  
- Functions called/Symbols referenced:
  - [geterrposition](../g/geterrposition.md)
  - [errposition](../e/errposition.md)
  - [internalerrposition](../i/internalerrposition.md)
  - [internalerrquery](../i/internalerrquery.md)
  - errcontext
- Called from (representative examples):
  - [fmgr_sql](../f/fmgr_sql.md) (registered as error callback)

## Notes and Other Information
- Registered as an error context callback in fmgr_sql to provide enhanced error reporting during SQL function execution
- Handles syntax error position translation from external source positions to internal query positions
- Provides different error context messages for different phases: during startup, during specific statement execution, or general function context
- Traverses execution states to identify the currently executing query when an error occurs
- Returns early if the function cache is not available or function name is not set, indicating very early initialization failure
- Helps developers debug SQL functions by clearly identifying which statement within a multi-statement function caused an error
- The callback mechanism integrates with PostgreSQL's error reporting system to provide contextual stack traces