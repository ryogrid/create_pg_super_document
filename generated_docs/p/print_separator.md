# print_separator

## Location
[src/fe_utils/print.c:379-397](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L379-L397)

## Overview
Outputs a separator character or string to a file stream, handling both null-terminated strings and null byte separators.

## Definition


## Detailed Description
The  function is a utility that outputs separator characters or strings to a specified file stream. It handles two types of separators: null byte separators (when  is true) and regular string separators. The function first checks if a null byte separator should be output using , otherwise it outputs a regular string separator using . This function is commonly used in unaligned text printing where different types of field separators are needed between data elements.

## Parameters / Member Variables
- : A  containing separator configuration with fields:
  - : Boolean flag indicating whether to output a null byte separator
  - : String pointer containing the separator text to output
- : File stream pointer where the separator should be written

## Dependencies
- Functions called/Symbols referenced:
  - fputc (standard C library function)
  - fputs (standard C library function)
  - [printTableFooter](printTableFooter.md) (referenced but relationship unclear from source)
- Called from (representative examples):
  - [print_unaligned_text](print_unaligned_text.md) (multiple locations)
  - [print_unaligned_vertical](print_unaligned_vertical.md) (multiple locations)

## Notes and Other Information
- This is a static function, only accessible within src/fe_utils/print.c
- Provides a unified interface for outputting different types of field separators
- The null byte separator option is useful for creating output that can be processed by tools expecting null-delimited data
- The function does not perform any validation on the separator structure or file stream
- Used extensively in unaligned printing functions for formatting tabular output