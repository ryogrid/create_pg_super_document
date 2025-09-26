# name_text

## Location
src/backend/utils/adt/varlena.c: 3382 - 3398

## Overview
Converts a PostgreSQL Name type to a text type, enabling interoperability between these two string data types.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that performs type conversion from the Name data type to the text data type. The Name type is a fixed-length string type used internally by PostgreSQL for system identifiers like table names, column names, etc., while text is a variable-length string type. This function extracts the null-terminated string from the Name type and converts it to a properly formatted text datum.

The conversion process involves:
1. Extracting the Name argument using the PostgreSQL function call interface
2. Converting the Name's string content to a null-terminated C string using NameStr
3. Converting the C string to a text datum using cstring_to_text
4. Returning the text datum

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - A single Name type argument (accessed via PG_GETARG_NAME(0))

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract Name argument from function call
  - : Macro to get null-terminated string from Name type
  - : Function to convert C string to text datum
  - : Macro to return text datum
  - : PostgreSQL Name data type

- Called from (representative examples):
  - : Name case-insensitive LIKE operation
  - : Name case-insensitive NOT LIKE operation

## Notes and Other Information
- This is a fundamental type conversion function in PostgreSQL's type system
- The function is typically used internally when PostgreSQL needs to convert Name types to text for comparison or output operations
- The Name type has a fixed maximum length (NAMEDATALEN), while text is variable-length
- Location: src/backend/utils/adt/varlena.c:3382-3398