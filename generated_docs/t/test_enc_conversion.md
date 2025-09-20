# test_enc_conversion

## Location
[src/test/regress/regress.c:1174-1290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/regress.c#L1174-L1290)

## Overview
A PostgreSQL regression test function that performs character encoding conversion between different encodings and returns both the number of successfully converted bytes and the converted result.

## Definition

```c
structure.
			 */
			Assert(oklen < srclen);
```
## Detailed Description
The  function is a PostgreSQL test utility that converts a byte string from one character encoding to another. It accepts a bytea input string, source encoding name, destination encoding name, and a boolean flag indicating whether to suppress errors. The function returns a composite type containing the number of bytes successfully converted and the converted bytea result.

The function handles two main scenarios:
1. **Same encoding conversion**: When source and destination encodings are identical, it validates the input string and returns it unchanged if valid
2. **Cross-encoding conversion**: Uses PostgreSQL's encoding conversion system to transform the string from source to destination encoding

The function includes comprehensive error handling, memory management, and supports both strict and lenient conversion modes based on the  parameter.

## Parameters / Member Variables
-  (bytea): The input byte string to be converted
-  (Name): The name of the source character encoding
-  (Name): The name of the destination character encoding  
-  (bool): If true, suppresses errors and returns partial results for invalid input

## Dependencies
- Functions called/Symbols referenced:
  - : Extract bytea parameter
  - : Extract Name parameter
  - : Extract boolean parameter
  - : Convert encoding name to encoding ID
  - : Verify multibyte string validity
  - : Find conversion function between encodings
  - : Perform actual encoding conversion
  - : Create return tuple
  - : Return result datum
- Called from (representative examples):
  - : Test setup function (src/test/regress/regress.c:1172)

## Notes and Other Information
- Located in the regression test suite ()
- Returns a composite type with two fields: converted byte count and converted bytea
- Handles memory allocation carefully to prevent overflow during conversion
- Uses  constant to estimate maximum output size
- Validates encoding names and reports appropriate errors for invalid encodings
- Supports partial conversion when  is true, returning valid prefix of input