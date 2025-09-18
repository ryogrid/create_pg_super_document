# personal_struct

## Location
[src/interfaces/ecpg/test/expected/preproc-variable.c:78-79](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/preproc-variable.c#L78-L79)

## Overview
A struct definition in ECPG test cases that represents a complete personal record containing name and birth information.

## Definition


## Detailed Description
The  is a composite data structure used in PostgreSQL's ECPG (Embedded SQL in C) test infrastructure. This struct demonstrates how the ECPG preprocessor handles complex nested structures that combine different data types. It contains both a variable-length string structure (varchar_1) for storing names and a birthinfo struct for storing temporal information.

The struct definition also declares two instances:  (a direct instance) and  (a pointer to the struct), showing how the preprocessor handles both direct struct usage and pointer declarations.

## Parameters / Member Variables
- : A varchar_1 struct containing length and character array for storing names
- : A birthinfo struct containing birth year and age information

## Dependencies
- Functions called/Symbols referenced:
  - [varchar_1](../v/varchar_1.md) (nested struct for name storage)
  - [birthinfo](../b/birthinfo.md) (nested struct for birth information)
- Called from (representative examples):
  - No direct references found

## Notes and Other Information
- Part of ECPG preprocessor test suite for testing complex struct handling
- Demonstrates nested struct composition in embedded SQL context
- The definition includes instances (personal, *p) showing practical usage patterns
- Located in expected output file for ECPG variable preprocessing tests
- Contains preprocessor line directives referencing original .pgc source
- Tests the preprocessor's ability to handle structures with both simple and complex member types