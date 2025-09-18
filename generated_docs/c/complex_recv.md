# complex_recv

## Location
src/tutorial/complex.c: 71 - 84

## Overview
Binary input function for the  data type that deserializes a complex number from PostgreSQL's binary message format.

## Definition


## Detailed Description
The  function is responsible for converting binary data received over PostgreSQL's network protocol into the internal  data type representation. It reads two consecutive 8-byte double-precision floating-point values from a StringInfo buffer, representing the real and imaginary parts of the complex number. This function is used for efficient binary data transfer in client-server communication and for storage in binary format.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  -  (accessed via ): StringInfo buffer containing the binary representation of the complex number

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract pointer argument
  - : PostgreSQL memory allocation function
  - : Function to extract 8-byte float from message buffer
  - : Macro to return pointer value
- Called from (representative examples):
  - : Referenced in the same file for function registration

## Notes and Other Information
- Counterpart to  for binary serialization/deserialization
- Reads binary data in network byte order (big-endian)
- More efficient than text-based input/output for bulk data operations
- Uses  to handle proper byte order conversion
- Allocates memory using PostgreSQL's palloc() for the result structure
- Part of the PostgreSQL tutorial demonstrating custom data type implementation
- Located in src/tutorial/complex.c:71-84