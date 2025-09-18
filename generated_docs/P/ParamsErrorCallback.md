# ParamsErrorCallback

## Location
src/backend/nodes/params.c: 407 - 422

## Overview
Error context callback function that prints parameter information during error reporting to provide better debugging context.

## Definition
void ParamsErrorCallback(void *arg)

## Detailed Description
This function serves as an error callback that adds parameter value information to error messages when parameter-related errors occur. It expects a ParamsErrorCbData structure as its argument and will print the parameter values string if available. The function only operates if BuildParamLogString has been previously called to populate the paramValuesStr field.

The callback distinguishes between named and unnamed portals, providing appropriate context information. It safely handles NULL arguments and missing parameter strings by returning early without action.

## Parameters / Member Variables
- : Pointer to ParamsErrorCbData structure containing:
  - : Name of the portal (can be NULL or empty for unnamed portals)
  - : ParamListInfo structure with paramValuesStr field populated

## Dependencies
- Functions called/Symbols referenced:
  - ParamsErrorCbData (callback data structure type)
  - errcontext (to add context information to error messages)
- Called from (representative examples):
  - exec_bind_message (during query parameter binding)
  - exec_execute_message (during query execution)

## Notes and Other Information
- Only provides output if paramValuesStr has been previously set by BuildParamLogString
- Differentiates between named and unnamed portals in error messages
- Safe to call with NULL or incomplete data structures
- Part of PostgreSQL's error reporting infrastructure for better parameter debugging
- Typically set up as an error context callback before operations that might fail with parameter-related errors