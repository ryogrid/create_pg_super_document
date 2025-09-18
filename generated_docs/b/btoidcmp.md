# btoidcmp

## Location
src/backend/access/nbtree/nbtcompare.c: 259 - 272

## Overview
A B-tree comparison function for PostgreSQL's OID (Object Identifier) data type that compares two OID values and returns an integer indicating their relative order.

## Definition


## Detailed Description
The btoidcmp function is a B-tree comparison function specifically designed for OID (Object Identifier) values in PostgreSQL. It takes two OID arguments through the PostgreSQL function call interface and performs a simple numeric comparison. The function returns a Datum containing an integer value that indicates the ordering relationship between the two OIDs: positive if the first OID is greater, zero if equal, or negative if the first is less than the second. This function is essential for B-tree index operations on OID columns, enabling efficient sorting and searching.

## Parameters / Member Variables
- : PostgreSQL's standard function argument interface containing:
  - First argument (index 0): OID value 'a' to compare
  - Second argument (index 1): OID value 'b' to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID: Extracts OID arguments from function call
  - PG_RETURN_INT32: Returns 32-bit integer result as Datum
  - A_GREATER_THAN_B: Constant indicating first argument is greater
  - A_LESS_THAN_B: Constant indicating first argument is less than second

- Called from (representative examples):
  - No direct references found in the codebase (likely referenced through function pointers in B-tree operator classes)

## Notes and Other Information
- This is a standard B-tree comparison function that follows PostgreSQL's comparison function contract
- Returns positive value when a > b, zero when a == b, and negative value when a < b
- Used internally by PostgreSQL's B-tree indexing system for OID data types
- The function is declared using PostgreSQL's V1 function call convention
- OIDs are unsigned 32-bit integers, so the comparison is straightforward numeric comparison