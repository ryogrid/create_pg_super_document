# FunctionParameterMode

## Location
src/include/nodes/parsenodes.h: 3449 - 3450

## Overview
FunctionParameterMode is an enumeration type that defines the different modes for function parameters in PostgreSQL, specifying whether parameters are input-only, output-only, bidirectional, or have special properties like variadic behavior.

## Definition


## Detailed Description
FunctionParameterMode defines the directional behavior and special characteristics of function parameters in PostgreSQL. The enum values are stored directly in the system catalog pg_proc and correspond to the parameter modes defined in the SQL standard:

- **FUNC_PARAM_IN**: Standard input parameter that passes values into the function
- **FUNC_PARAM_OUT**: Output parameter that returns values from the function without taking input
- **FUNC_PARAM_INOUT**: Bidirectional parameter that both accepts input and returns output
- **FUNC_PARAM_VARIADIC**: Variadic parameter that accepts a variable number of arguments (always input)
- **FUNC_PARAM_TABLE**: Special mode for table function output columns
- **FUNC_PARAM_DEFAULT**: Default mode (functionally equivalent to IN but not stored in pg_proc)

The character values ('i', 'o', 'b', 'v', 't') are the actual values stored in the pg_proc system catalog, making them part of PostgreSQL's persistent metadata format.

## Parameters / Member Variables
- : Input-only parameter (value 'i')
- : Output-only parameter (value 'o')  
- : Input/output parameter (value 'b')
- : Variadic input parameter (value 'v')
- : Table function output column (value 't')
- : Default mode, equivalent to IN (value 'd')

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enumeration type)
- Called from (representative examples):
  - [interpret_function_parameter_list](../i/interpret_function_parameter_list.md) (src/backend/commands/functioncmds.c:225)
  - [interpret_function_parameter_list](../i/interpret_function_parameter_list.md) (src/backend/commands/functioncmds.c:364)
  - FunctionParameter (src/include/nodes/parsenodes.h:3456)

## Notes and Other Information
- The enum values are explicitly assigned character values that are stored in the pg_proc system catalog
- These values must not be changed as they are part of PostgreSQL's persistent data format
- FUNC_PARAM_DEFAULT is not stored in pg_proc but is used during parsing/processing
- The mode determines how parameters are handled during function calls and how they appear in function signatures
- Table functions use FUNC_PARAM_TABLE to define output column specifications
- Variadic parameters allow functions to accept a variable number of arguments of the same type