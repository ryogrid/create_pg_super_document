# map_xml_name_to_sql_identifier

## Location
src/backend/utils/adt/xml.c: 2436 - 2477

## Overview
Maps XML names back to SQL identifiers by reversing the character escaping performed by , implementing SQL/XML:2008 section 9.3.

## Definition


## Detailed Description
This function converts XML names back to their original SQL identifier form by decoding escape sequences that were applied during the XML name mapping process. It specifically looks for the Unicode escape pattern  where NNNN represents a 4-digit hexadecimal Unicode code point, and converts these back to their original characters.

The function processes the input character by character, detecting escape sequences and converting them back to their original Unicode characters using the server's character encoding. Characters that were not escaped are copied directly to the output.

This function serves as the inverse operation to , allowing for round-trip conversion between SQL identifiers and XML names.

## Parameters
- : The XML name string to be converted back to a SQL identifier

## Dependencies
- Functions called/Symbols referenced:
  - : Initialize string buffer for building result
  - : Get multibyte character length for proper iteration
  - : Check if character is a hexadecimal digit (from standard C library)
  - : Parse hexadecimal Unicode value from escape sequence
  - : Convert Unicode code point to server encoding
  - : Append decoded character to result buffer
  - : Append original character bytes when no decoding needed
  - : Maximum length constant for Unicode character conversion

- Called from (representative examples):
  - : Rule expression formatting in ruleutils
  - Functions that need to reverse XML name mapping back to SQL identifiers

## Notes and Other Information
- Returns a newly allocated string that must be freed by the caller
- Only processes escape sequences in the exact format  where N is a hexadecimal digit
- Supports multibyte character encodings through proper use of 
- The function assumes the input was previously processed by  or follows the same escape sequence format
- Characters that don't match the escape pattern are copied unchanged, making the function safe to use on strings that may not contain escape sequences