# varchar_3

## Location
[src/interfaces/ecpg/test/expected/preproc-array_of_struct.c:98-104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/preproc-array_of_struct.c#L98-L104)

## Overview
The  symbol is a nested struct definition that implements a variable-length character string type within the customer3 structure for ECPG testing.

## Definition


## Detailed Description
The  struct represents PostgreSQL's varchar data type implementation in C for embedded SQL applications. This structure provides a way to handle variable-length character strings efficiently by storing both the actual string length and the character data. The struct is used as a nested type within the customer3 structure to represent customer name fields with proper length tracking.

## Parameters / Member Variables
- : Integer field that stores the actual length of the string data in the array
- : Character array with a fixed maximum capacity of 50 characters for storing the string data
- : Instance variable of type varchar_3 used within the customer3 structure

## Dependencies
- Functions called/Symbols referenced: None (basic struct definition with primitive types)
- Called from (representative examples):
  - Used as a nested type within customer3 structure
  - Part of ECPG test suite for array_of_struct scenarios

## Notes and Other Information
- This is a typical PostgreSQL varchar implementation pattern where length is stored separately from the character data
- The 50-character limit is for testing purposes and demonstrates capacity constraints
- Used exclusively within ECPG test files to validate proper varchar handling in embedded SQL
- The separation of length and data allows for efficient string operations and memory management
- This pattern is common in PostgreSQL's internal string handling for performance optimization
- Forms part of the testing framework for ensuring ECPG properly handles variable-length character types