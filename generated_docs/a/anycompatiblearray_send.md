# anycompatiblearray_send

## Location
[src/backend/utils/adt/pseudotypes.c:184-196](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pseudotypes.c#L184-L196)

## Overview
A wrapper function that provides binary output capability for the anycompatiblearray pseudotype by delegating to the array_send function.

## Definition
Datum anycompatiblearray_send(PG_FUNCTION_ARGS)

## Detailed Description
The anycompatiblearray_send function serves as a binary output function for the anycompatiblearray pseudotype in PostgreSQL. It acts as a thin wrapper around the array_send function, simply forwarding the function call information (fcinfo) to array_send to handle the actual binary serialization. This design allows the anycompatiblearray pseudotype to leverage the existing array binary output infrastructure without duplicating code. The anycompatiblearray pseudotype is part of PostgreSQL's enhanced polymorphic type system that ensures type compatibility across multiple polymorphic parameters in function calls.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: Standard PostgreSQL function call information macro that provides access to function arguments and context

## Dependencies
- Functions called/Symbols referenced:
  - [array_send](array_send.md): The actual implementation for binary array output
  - PSEUDOTYPE_DUMMY_INPUT_FUNC: Referenced in the surrounding context
- Called from (representative examples):
  - No direct references found in the codebase (typically called through PostgreSQL's type system)

## Notes and Other Information
- Located in src/backend/utils/adt/pseudotypes.c:184-196
- Part of PostgreSQL's enhanced pseudotype system for handling polymorphic types with compatibility constraints
- The anycompatiblearray pseudotype works with other anycompatible* types to ensure consistent type resolution
- Binary output functions are used for network transmission, replication, and storage of data values
- This function complements anycompatiblearray_out by providing binary serialization instead of text output
- Essential for PostgreSQL's client-server protocol when transmitting anycompatiblearray values in binary format