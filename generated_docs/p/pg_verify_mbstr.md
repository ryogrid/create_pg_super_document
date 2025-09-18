# pg_verify_mbstr

## Location
[src/backend/utils/mb/mbutils.c:1566-1596](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L1566-L1596)

## Overview
Verifies that a multibyte string is validly encoded in the specified character encoding.

## Definition


## Detailed Description
This function validates whether a given multibyte string conforms to the encoding rules of the specified character encoding. It uses the encoding-specific verification function from the pg_wchar_table to check the entire string byte by byte. If an invalid sequence is found, the function can either report an error or return false based on the noError parameter.

The function operates by calling the encoding-specific mbverifystr function pointer from the pg_wchar_table array, which returns the length of the valid portion of the string. If this length doesn't match the input length, it indicates an encoding violation.

## Parameters / Member Variables
- : Integer identifier for the target character encoding (must be valid according to PG_VALID_ENCODING)
- : Pointer to the multibyte string to be verified
- : Length of the string in bytes to verify
- : Boolean flag controlling error handling behavior - if true, returns false on invalid encoding; if false, reports error via report_invalid_encoding

## Dependencies
- Functions called/Symbols referenced:
  - PG_VALID_ENCODING (macro for encoding validation)
  - pg_wchar_table[encoding].mbverifystr (encoding-specific verification function)
  - [report_invalid_encoding](../r/report_invalid_encoding.md) (error reporting function)
- Called from (representative examples):
  - [AddFileToBackupManifest](../A/AddFileToBackupManifest.md)
  - [read_extension_script_file](../r/read_extension_script_file.md)
  - [LogicalOutputWrite](../L/LogicalOutputWrite.md)
  - [pg_do_encoding_conversion](pg_do_encoding_conversion.md)
  - [pg_convert](pg_convert.md)
  - [pg_any_to_server](pg_any_to_server.md)
  - [pg_server_to_any](pg_server_to_any.md)
  - [pg_verifymbstr](pg_verifymbstr.md)

## Notes and Other Information
- The function assumes the encoding parameter is valid and uses an assertion to check this
- Returns true if the entire string is valid, false only when noError is true and invalid encoding is detected
- When noError is false, the function will not return false but will instead call report_invalid_encoding and potentially terminate execution
- The verification is performed by encoding-specific functions that understand the byte sequence rules for each supported encoding