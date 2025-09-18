# ParamsErrorCbData

## Location
src/include/nodes/params.h: 154 - 158

## Overview
ParamsErrorCbData is a struct that serves as the argument for parameter error callback functions, providing context information for error reporting when parameter-related issues occur during query execution.

## Definition


## Detailed Description
ParamsErrorCbData is specifically designed as the argument type for ParamsErrorCallback functions. It encapsulates the essential context needed for meaningful error reporting when parameter-related errors occur during query preparation or execution. This structure provides both the portal name for identifying the specific query context and the parameter list for detailed parameter information in error messages.

The structure is primarily used in PostgreSQL's frontend/backend protocol handling, particularly during Bind and Execute message processing, where parameter binding errors need to be reported with sufficient context for debugging.

## Parameters / Member Variables
- : Name of the portal (prepared statement cursor) where the parameter error occurred, used for error context identification
- : Pointer to the ParamListInfo structure containing the parameter list that caused the error

## Dependencies
- Functions called/Symbols referenced:
  - ParamListInfo

- Called from (representative examples):
  - ParamsErrorCallback (parameter error callback function)
  - exec_bind_message (Bind message processing in postgres.c)
  - exec_execute_message (Execute message processing in postgres.c)

## Notes and Other Information
- Used exclusively for error callback context in parameter processing
- Critical for providing meaningful error messages when parameter binding fails
- Helps identify both the query context (via portalName) and specific parameter issues (via params)
- Part of PostgreSQL's error reporting infrastructure for the frontend/backend protocol
- Enables better debugging of parameter-related issues in prepared statements and portals