# get_hexdigit

## Location
src/interfaces/libpq/fe-connect.c: 6817 - 6837

## Overview
Converts a single hexadecimal digit character (0-9, A-F, a-f) to its corresponding integer value (0-15).

## Definition


## Detailed Description
This utility function provides case-insensitive conversion of hexadecimal digit characters to their numeric values. It supports all valid hexadecimal characters:
- Digits 0-9: converted to values 0-9
- Upper-case letters A-F: converted to values 10-15  
- Lower-case letters a-f: converted to values 10-15

The function validates the input character and returns false for any character that is not a valid hexadecimal digit. This makes it suitable for use in URI percent-decoding where malformed hex sequences need to be detected and reported as errors.

## Parameters / Member Variables
- : The character to convert (must be a valid hexadecimal digit)
- : Pointer to integer where the converted value (0-15) will be stored

## Dependencies
- Functions called/Symbols referenced:
  - (none - uses only basic character operations)
- Called from (representative examples):
  - [conninfo_uri_decode](../c/conninfo_uri_decode.md)
  - internalPQconninfoOption

## Notes and Other Information
- Returns true on successful conversion, false for invalid characters
- Case-insensitive: treats 'A' and 'a' identically
- Output value is guaranteed to be in range 0-15 when function returns true
- Does not modify the value parameter if conversion fails
- Essential component for URI percent-decoding (%xy sequences)
- Simple and efficient character-to-integer conversion without external dependencies