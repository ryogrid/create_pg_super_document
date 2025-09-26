# pg_encoding

## Location
[src/backend/utils/adt/encode.c:33-47](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/encode.c#L33-L47)

## Overview
The pg_encoding struct defines a generic interface for encoding conversion operations in PostgreSQL, providing function pointers for length estimation and actual encoding/decoding of binary data.

## Definition

```c
struct pg_encoding
{
	uint64		(*encode_len) (const char *data, size_t dlen);
	uint64		(*decode_len) (const char *data, size_t dlen);
	uint64		(*encode) (const char *data, size_t dlen, char *res);
	uint64		(*decode) (const char *data, size_t dlen, char *res);
};
```
## Detailed Description
The pg_encoding struct serves as an abstraction layer for various encoding schemes (hex, base64, escape) used in PostgreSQL's binary data handling. This design allows different encoding methods to be implemented uniformly through a common interface, where each encoding type provides its own implementation of the four required operations.

The struct is used internally by PostgreSQL's encoding conversion system to support SQL functions like encode() and decode() that convert binary data to and from various text representations. The function pointer approach enables runtime selection of the appropriate encoding method based on the encoding name provided by the user.

The API is designed with safety in mind - the length estimation functions (_len functions) are allowed to return overestimates but not underestimates, ensuring sufficient buffer space is allocated before the actual conversion operations.

## Parameters / Member Variables
- : Function pointer to estimate the output length needed for encoding the given input data
- : Function pointer to estimate the output length needed for decoding the given input data  
- : Function pointer to perform the actual encoding conversion from binary to text format
- : Function pointer to perform the actual decoding conversion from text to binary format

## Dependencies
- Functions called/Symbols referenced:
  - Used by  function to locate encoding implementations
- Called from (representative examples):
  -  function at src/backend/utils/adt/encode.c:58
  -  function at src/backend/utils/adt/encode.c:106
  -  function references at src/backend/utils/adt/encode.c:572,602

## Notes and Other Information
- The struct is used in a static lookup table (enclist) that maps encoding names to their implementations
- Current supported encodings include "hex", "base64", and "escape"
- The encode_len and decode_len functions may return overestimates for safety, but large overestimates should be avoided to prevent unnecessary memory allocation errors
- The actual conversion functions (encode/decode) must return the true length of their output
- This interface is located in src/backend/utils/adt/encode.c:33-47 and is part of PostgreSQL's ADT (Abstract Data Type) utilities