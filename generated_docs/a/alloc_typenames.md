# alloc_typenames

## Location
src/tools/pg_bsd_indent/lexi.c: 677 - 686

## Overview
Initializes dynamic memory allocation for the typenames array used to store user-defined type names during code formatting.

## Definition
void alloc_typenames(void)

## Detailed Description
This function allocates initial memory for the global typenames array, which stores user-defined type names that pg_bsd_indent uses for proper code formatting. The function allocates space for an initial capacity of 16 type name pointers and sets the typename_count to this initial size. If memory allocation fails, the program terminates with an error message. This is typically called during program initialization before processing any input files.

## Parameters / Member Variables
None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - malloc (standard C library function for memory allocation)
  - err (error reporting and program termination)
- Called from (representative examples):
  - main (at src/tools/pg_bsd_indent/indent.c:112)

## Notes and Other Information
- Allocates initial capacity for 16 typename pointers
- Sets global typename_count variable to initial capacity
- Program exits with error code 1 if malloc fails
- Part of the initialization sequence for pg_bsd_indent
- The typenames array can be dynamically expanded later as needed
- Essential for supporting user-defined type recognition during formatting