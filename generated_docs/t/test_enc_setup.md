# test_enc_setup

## Location
[src/test/regress/regress.c:1111-1173](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/regress.c#L1111-L1173)

## Overview
A PostgreSQL test function that validates the behavior of encoding-related functions, specifically testing the setup and validation of invalid multibyte character strings across all supported encodings.

## Definition


## Detailed Description
This function performs comprehensive testing of PostgreSQL's character encoding infrastructure, specifically focusing on the behavior of invalid character sequences. The function iterates through all available encodings that support multibyte characters (those with maximum length > 1) and tests the following aspects:

1. **Invalid String Generation**: Uses  to create official invalid character sequences for each encoding
2. **Length Validation**: Verifies that invalid strings have the expected length (2 bytes)
3. **Multibyte Length Checking**: Confirms that  correctly identifies the length of invalid sequences
4. **String Verification**: Tests  with various scenarios:
   - Full invalid string (should return 0 for no valid prefix)
   - First byte only (should return 0 for no valid prefix) 
   - Invalid string with trailing data (should still return 0)

The function serves as a regression test to ensure that PostgreSQL's encoding validation functions behave consistently and correctly identify invalid multibyte sequences across all supported character encodings.

## Parameters / Member Variables
This function uses the standard PostgreSQL function interface:
- Uses  macro for parameter handling (no specific parameters required)
- Returns  using  as it performs validation testing only

## Dependencies
- Functions called/Symbols referenced:
  - : Constant defining the total number of supported encodings
  - : Returns maximum byte length for characters in an encoding
  - : Creates an official invalid character sequence for an encoding
  - : Standard C library function to get string length with maximum limit
  - : Returns the byte length of a multibyte character
  - : Validates a multibyte string and returns length of valid prefix
  - : Table mapping encoding IDs to encoding names
  - : PostgreSQL logging function for emitting warnings
  - : Standard C library function for memory initialization
- Called from (representative examples):
  - Referenced by test_opclass_options_func at src/test/regress/regress.c:1109

## Notes and Other Information
- This function is part of PostgreSQL's regression test suite located in 
- The function only tests multibyte encodings (those with max length > 1), skipping single-byte encodings
- All validation failures are reported as WARNING level log messages rather than errors
- The function tests edge cases like partial invalid strings and invalid strings with trailing data
- This testing is crucial for ensuring data integrity and preventing security issues related to character encoding validation
- The function demonstrates PostgreSQL's robust approach to character encoding handling
- Proper encoding validation is essential for preventing issues like character set confusion attacks
- The use of WARNING level logging allows the test to continue even if some encodings have unexpected behavior