# replace_variables

## Location
src/interfaces/ecpg/ecpglib/prepare.c: 104 - 158

## Overview
A static function that processes SQL text to replace named parameters (e.g., :param or ?param) with PostgreSQL-style positional parameters (e.g., $1, $2).

## Definition


## Detailed Description
The `replace_variables` function transforms SQL statements containing named parameters into PostgreSQL's numbered parameter format. It scans through the input text character by character, identifying parameter markers (: or ?) while properly handling string literals to avoid replacing parameters within quoted strings. When a parameter is found, it replaces the parameter name with a numbered placeholder ($1, $2, etc.) and reallocates the string to accommodate the new format.

## Parameters / Member Variables
- `text`: A double pointer to the SQL text string to be processed; modified in-place with the transformed text
- `lineno`: Line number for error reporting and memory allocation tracking

## Dependencies
- Functions called/Symbols referenced:
  - ecpg_alloc
  - ecpg_free
  - isvarchar
  - snprintf
  - memcpy
  - strcpy
  - strcat
  - strlen
- Called from (representative examples):
  - prepare_common

## Notes and Other Information
- This is a static function local to the prepare.c file in the ECPG library
- Returns true on success, false on memory allocation failure
- Handles string literals properly by tracking quote state to avoid replacing parameters inside strings
- Skips PostgreSQL's double-colon (::) cast operator to avoid false parameter detection
- Uses a counter to assign sequential numbers to parameters ($1, $2, etc.)
- Performs dynamic memory reallocation to accommodate the text changes
- Handles edge cases where parameter replacement might reach the end of the string
- The function modifies the original text pointer, replacing it with a newly allocated string