# PLy_procedure_is_trigger

## Location
src/pl/plpython/plpy_main.c: 339 - 344

## Overview
PLy_procedure_is_trigger is a static utility function that determines whether a PostgreSQL procedure is defined as a trigger function.

## Definition
static bool PLy_procedure_is_trigger(Form_pg_proc procStruct)

## Detailed Description
This function provides a simple check to determine if a given PostgreSQL procedure is intended to be used as a trigger function. It examines the procedure's metadata to identify trigger functions by checking the return type. Trigger functions in PostgreSQL must return the TRIGGER type, which is identified by the TRIGGEROID constant.

This function is used internally within the PL/Python language handler to differentiate between regular functions and trigger functions, allowing the system to apply appropriate handling and validation logic for each type.

## Parameters / Member Variables
- : A Form_pg_proc structure containing the procedure's metadata from the pg_proc system catalog, including return type information

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_proc: PostgreSQL system catalog structure type
  - TRIGGEROID: PostgreSQL built-in type identifier for trigger return type
- Called from (representative examples):
  - [plpython3_validator](../p/plpython3_validator.md): Used during procedure validation to determine trigger vs regular function handling

## Notes and Other Information
- Located in src/pl/plpython/plpy_main.c:339-344
- This is a static function, meaning it's only accessible within the same compilation unit
- The function performs a simple comparison: returns true if prorettype equals TRIGGEROID, false otherwise
- Essential for proper categorization of PL/Python functions during validation and execution phases
- Trigger functions have different parameter passing conventions and execution contexts compared to regular functions