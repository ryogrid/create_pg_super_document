# CopyReadBinaryAttribute

## Location
src/backend/commands/copyfromparse.c: 1986 - 2032

## Overview
Reads and processes a single binary attribute from binary-format COPY data, handling the binary protocol's length-prefixed format and invoking the appropriate type input function to convert raw bytes to PostgreSQL datum values.

## Definition


## Detailed Description
This function implements the binary COPY protocol for individual field processing in PostgreSQL. Binary COPY format uses a length-prefixed protocol where each field is preceded by a 4-byte signed integer indicating the field's byte length. The function handles the complete binary attribute reading pipeline from length parsing to type conversion.

The binary protocol specifications:
- **Field length**: 4-byte signed integer in network byte order
- **Special values**: -1 indicates NULL, negative values (except -1) are invalid
- **Field data**: Raw bytes following the length header
- **Type conversion**: Binary data is passed to the type's binary receive function

The function provides robust error handling for truncated data, invalid lengths, and incomplete type conversions. It ensures that the type's receive function consumes exactly the expected number of bytes, detecting malformed binary data.

Key operational steps:
1. Read the 4-byte length prefix using network byte order
2. Handle NULL values (length -1) immediately
3. Validate field length is non-negative
4. Allocate buffer space and read the binary field data
5. Invoke the type-specific binary receive function
6. Verify complete data consumption by the receive function

## Parameters / Member Variables
- : The COPY operation state containing input buffers and parsing context
- : Function manager info for the type's binary receive function, containing the function pointer and metadata for efficient invocation
- : Type-specific parameter passed to the receive function, typically the element type OID for arrays or composite types
- : Type modifier value providing additional type constraints (e.g., precision for NUMERIC, length for VARCHAR)
- : Output parameter set to true for NULL values, false for non-NULL data

## Dependencies
- Functions called/Symbols referenced:
  - : Reads 4-byte signed integer from input stream in network byte order
  - : Invokes type-specific binary receive function with proper error handling
  - : Clears the attribute buffer for reuse
  - : Ensures sufficient buffer capacity for the field data
  - : Reads exact number of bytes from input stream with EOF detection
- Called from (representative examples):
  - : Main binary COPY processing loop for tuple assembly

## Notes and Other Information
- Binary format is significantly more efficient than text formats as it avoids parsing and formatting overhead
- The function assumes network byte order (big-endian) for the length prefix, ensuring cross-platform compatibility
- NULL handling is done at the protocol level (length -1) rather than through string comparison like text formats
- Type receive functions are expected to consume exactly the provided number of bytes; partial consumption indicates data corruption
- Buffer management is optimized to reuse the attribute_buf across multiple field reads within the same row
- Error messages distinguish between protocol-level issues (unexpected EOF, invalid length) and type-level issues (incorrect binary representation)
- The function integrates with PostgreSQL's function manager system for efficient type conversion dispatch