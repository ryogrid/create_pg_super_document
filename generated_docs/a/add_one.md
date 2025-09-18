# add_one

## Location
[src/tutorial/funcs.c:24-35](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tutorial/funcs.c#L24-L35)

## Overview
A simple PostgreSQL C function that increments an integer argument by one, serving as a basic example in the PostgreSQL tutorial for creating user-defined functions.

## Definition


## Detailed Description
The  function is a PostgreSQL C function that takes a single 32-bit integer parameter and returns the value incremented by 1. This function is part of the PostgreSQL tutorial examples, demonstrating the basic structure and conventions for writing PostgreSQL C functions. It uses the standard PostgreSQL function calling conventions with  macro for parameter handling and  for returning values.

## Parameters / Member Variables
- : Standard PostgreSQL macro that provides access to function arguments and context information
  - The function extracts the first argument as a 32-bit integer using 

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract a 32-bit integer argument
  - : Macro to return a 32-bit integer value
  - : Macro for function metadata (referenced at line 33)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in 
- This is a tutorial example function demonstrating PostgreSQL C function basics
- The function follows PostgreSQL's version 1 calling convention
- Uses standard PostgreSQL macros for argument handling and return values
- Serves as a template for more complex user-defined functions