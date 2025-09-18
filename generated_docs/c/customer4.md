# customer4

## Location
[src/interfaces/ecpg/test/expected/preproc-array_of_struct.c:105-106](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/preproc-array_of_struct.c#L105-L106)

## Overview
The  symbol is a struct definition used in ECPG (Embedded SQL in C for PostgreSQL) test cases, similar to customer3 but instantiated as a single object rather than an array.

## Definition


## Detailed Description
The  struct represents another variant of a customer record structure designed for ECPG testing. It demonstrates the use of variable-length character fields (varchar_4) within embedded SQL applications. The structure is identical to customer3 in terms of member types but uses varchar_4 for the name field and is instantiated as a single object (custs4) rather than an array. This provides test coverage for different instantiation patterns.

## Parameters / Member Variables
- : A nested varchar_4 structure containing length information and character array for storing customer names
  - : Integer representing the actual length of the string
  - : Character array with maximum capacity of 50 characters
- : Integer field for storing customer phone numbers

## Dependencies
- Functions called/Symbols referenced:
  - [varchar_4](../v/varchar_4.md) (nested struct type)
- Called from (representative examples):
  - Referenced in varchar_5 operations
  - Used in pointer_to_struct test scenarios
  - Referenced from varchar_3 in pointer_to_struct test files

## Notes and Other Information
- This is part of the ECPG test suite demonstrating proper handling of variable-length strings in embedded SQL
- Unlike customer3 which is declared as an array, customer4 is instantiated as a single object (custs4)
- The varchar_4 pattern maintains the same structure as varchar_3 but allows testing of different varchar type variations
- Used primarily in preprocessing and compilation testing for ECPG applications
- Demonstrates PostgreSQL's flexible approach to handling different instantiation patterns for the same basic structure
- Part of comprehensive testing to ensure ECPG handles both array and single object scenarios correctly