# btint8cmp

## Location
[src/backend/access/nbtree/nbtcompare.c:132-146](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtcompare.c#L132-L146)

## Overview
A PostgreSQL B-tree comparison function for 64-bit integer (int8/bigint) data types that returns the comparison result between two int64 values.

## Definition


## Detailed Description
This function implements the standard three-way comparison for 64-bit signed integers in PostgreSQL B-tree indexes. It takes two int64 arguments and returns an integer indicating their relative ordering: positive if the first argument is greater, zero if equal, or negative if the first is less than the second. This comparison function is essential for B-tree index operations on bigint columns.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro providing access to two int64 parameters:
  - First parameter: Left-hand side int64 value for comparison
  - Second parameter: Right-hand side int64 value for comparison

## Dependencies
- Functions called/Symbols referenced:
  -  (PostgreSQL argument extraction macro)
  -  (comparison result constant)
  -  (comparison result constant)
  -  (size constant)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's B-tree index support infrastructure for 64-bit integers
- Located in 
- Returns standard comparison values: >0 for greater than, 0 for equal, <0 for less than
- Used internally by the B-tree access method for organizing and searching bigint values in indexes