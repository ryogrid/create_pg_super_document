# customer3

## Location
src/interfaces/ecpg/test/expected/preproc-array_of_struct.c: 96 - 97

## Overview
The  symbol is a struct definition used in ECPG (Embedded SQL in C for PostgreSQL) test cases for handling customer data with variable-length character fields.

## Definition


## Detailed Description
The  struct represents a customer record structure designed for ECPG testing. It demonstrates the use of variable-length character fields (varchar) within embedded SQL applications. The structure includes a nested varchar_3 struct for the name field and a simple integer for the phone number. This is instantiated as an array of 10 elements named .

## Parameters / Member Variables
- : A nested varchar_3 structure containing length information and character array for storing customer names
  - : Integer representing the actual length of the string
  - : Character array with maximum capacity of 50 characters
- : Integer field for storing customer phone numbers

## Dependencies
- Functions called/Symbols referenced:
  - [varchar_3](../v/varchar_3.md) (nested struct type)
- Called from (representative examples):
  - Referenced in varchar_5 operations
  - Used in pointer_to_struct test scenarios
  - Part of array_of_struct test patterns

## Notes and Other Information
- This is part of the ECPG test suite demonstrating proper handling of variable-length strings in embedded SQL
- The varchar pattern with separate length field is common in PostgreSQL for efficient string handling
- Declared as an array of 10 elements, typical for test data scenarios
- The structure shows PostgreSQL's approach to handling variable-length character data in C programs
- Used primarily in preprocessing and compilation testing for ECPG applications