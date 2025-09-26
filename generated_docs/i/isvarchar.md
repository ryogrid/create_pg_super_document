# isvarchar

## Location
[src/interfaces/ecpg/ecpglib/prepare.c:44-58](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/prepare.c#L44-L58)

## Overview
A static utility function that determines whether a character is valid for use in a variable name context within ECPG (Embedded SQL in C for PostgreSQL).

## Definition

```c
static bool
isvarchar(unsigned char c)
```
## Detailed Description
The  function checks if a given character is considered valid for variable name parsing in ECPG. It extends the standard alphanumeric character set to include specific special characters that are commonly used in PostgreSQL identifiers and variable references. The function handles both ASCII and extended character sets (characters with values >= 128).

## Parameters / Member Variables
- : An unsigned character to be tested for validity in variable names

## Dependencies
- Functions called/Symbols referenced:
  - isalnum (standard C library function)
- Called from (representative examples):
  - replace_variables

## Notes and Other Information
- This is a static function local to the prepare.c file in the ECPG library
- The function allows characters beyond standard alphanumeric: underscore (_), greater than (>), hyphen (-), and period (.)
- Characters with ASCII values >= 128 are considered valid, allowing for extended character sets
- Used primarily in variable name parsing and replacement operations within ECPG prepared statements