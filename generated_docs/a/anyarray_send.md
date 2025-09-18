# anyarray_send

## Location
[src/backend/utils/adt/pseudotypes.c:164-177](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pseudotypes.c#L164-L177)

## Overview
A wrapper function that provides binary output capability for the anyarray pseudotype by delegating to the array_send function.

## Definition


## Detailed Description
The anyarray_send function serves as a binary output function for the anyarray pseudotype in PostgreSQL. It acts as a thin wrapper around the array_send function, simply forwarding the function call information (fcinfo) to array_send to handle the actual binary serialization. This design allows the anyarray pseudotype to leverage the existing array binary output infrastructure without duplicating code.

## Parameters / Member Variables
- : Standard PostgreSQL function call information macro that provides access to function arguments and context

## Dependencies
- Functions called/Symbols referenced:
  - [array_send](array_send.md): The actual implementation for binary array output
- Called from (representative examples):
  - No direct references found in the codebase (typically called through PostgreSQL's type system)

## Notes and Other Information
- Located in src/backend/utils/adt/pseudotypes.c:164-177
- Part of PostgreSQL's pseudotype system for handling polymorphic types
- The anyarray pseudotype allows functions to accept arrays of any element type
- Binary output functions are used for network transmission and storage of data values