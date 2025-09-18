# deccall2

## Location
src/interfaces/ecpg/compatlib/informix.c: 48 - 85

## Overview
A static utility function that serves as a wrapper to call numeric functions that take two parameters and return an integer result, handling memory management and decimal-to-numeric conversion.

## Definition


## Detailed Description
The  function is an internal helper function in the ECPG Informix compatibility library that provides a standardized way to call numeric functions requiring two input parameters. It handles the conversion from Informix decimal types to PostgreSQL numeric types, manages memory allocation and cleanup, and executes the provided function pointer. The function ensures proper resource management by cleaning up allocated numeric values regardless of success or failure.

## Parameters / Member Variables
- : Pointer to the first decimal input argument to be converted to numeric type
- : Pointer to the second decimal input argument to be converted to numeric type  
- : Function pointer to the numeric function to be called with the converted arguments

## Dependencies
- Functions called/Symbols referenced:
  - PGTYPESnumeric_new
  - PGTYPESnumeric_free
  - PGTYPESnumeric_from_decimal
  - ECPG_INFORMIX_OUT_OF_MEMORY
- Called from (representative examples):
  - deccmp

## Notes and Other Information
- This is a static function internal to the Informix compatibility library
- Provides consistent error handling for out-of-memory conditions
- Always cleans up allocated memory before returning
- Returns the result of the called function pointer or error codes on failure
- Part of the ECPG interface for Informix decimal type compatibility