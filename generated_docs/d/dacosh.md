# dacosh

## Location
src/backend/utils/adt/float.c: 2682 - 2706

## Overview
The dacosh function computes the inverse hyperbolic cosine of a floating-point number with domain validation, providing PostgreSQL-specific error handling for mathematical operations.

## Definition
Datum dacosh(PG_FUNCTION_ARGS)

## Detailed Description
The dacosh function is a PostgreSQL wrapper around the standard C library acosh() function that calculates the inverse hyperbolic cosine (also known as area hyperbolic cosine) of a given floating-point argument. The function includes explicit domain validation since acosh is only mathematically defined for inputs >= 1.0. By performing this validation explicitly, PostgreSQL avoids relying on system-specific EDOM error handling, which can vary across implementations especially for NaN inputs. When the input is less than 1.0, the function raises a PostgreSQL error with an appropriate error code and message. For valid inputs, it computes the inverse hyperbolic cosine using the system acosh() function.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: Standard PostgreSQL function call context containing the input argument
- arg1: The input float8 value for which to compute the inverse hyperbolic cosine (must be >= 1.0)
- result: The computed inverse hyperbolic cosine result

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8: Extracts float8 argument from function call context
  - ereport: Reports PostgreSQL errors
  - errcode: Sets error code for out of range values
  - errmsg: Sets error message text
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function explicitly validates that input >= 1.0 since acosh is undefined for smaller values
- Domain validation prevents reliance on inconsistent system EDOM error handling
- Special consideration for NaN inputs which some implementations handle differently
- The function is part of PostgreSQL mathematical function library in src/backend/utils/adt/float.c
- Located at src/backend/utils/adt/float.c:2682-2706
- Error code ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE is used for invalid domain inputs