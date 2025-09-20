# customer3

## Location
[src/interfaces/ecpg/test/expected/preproc-array_of_struct.c:96-97](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/preproc-array_of_struct.c#L96-L97)

## Overview
The  symbol is a struct definition used in ECPG (Embedded SQL in C for PostgreSQL) test cases for handling customer data with variable-length character fields.

## Definition

```c
struct customer3 { 
#line 36 "array_of_struct.pgc"
  struct varchar_3  { int len; char arr[ 50 ]; }  name ;
 
#line 37 "array_of_struct.pgc"
 int phone ;
 } custs3 [ 10 ] ;
```
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