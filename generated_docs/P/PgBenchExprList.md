# PgBenchExprList

## Location
[src/bin/pgbench/pgbench.h:108-109](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.h#L108-L109)

## Overview
PgBenchExprList is a list management structure that maintains head and tail pointers for efficiently managing linked lists of PgBenchExprLink nodes.

## Definition


## Detailed Description
PgBenchExprList provides an efficient way to manage linked lists of expressions by maintaining both head and tail pointers. This design pattern allows for O(1) insertion at both the beginning and end of the list, which is important for building argument lists during expression parsing. The structure serves as a container for PgBenchExprLink chains, facilitating the construction and manipulation of expression sequences in pgbench scripts.

## Parameters / Member Variables
- : Pointer to the first PgBenchExprLink node in the list, or NULL if the list is empty
- : Pointer to the last PgBenchExprLink node in the list, or NULL if the list is empty

## Dependencies
- Functions called/Symbols referenced:
  - [PgBenchExprLink](PgBenchExprLink.md) (struct)
- Called from (representative examples):
  - Expression parsing functions
  - Function argument list construction

## Notes and Other Information
- Forward declared at line 108 in pgbench.h, with full definition at lines 135-139
- Implements a standard doubly-pointed linked list management pattern
- Provides efficient list operations for expression list construction during parsing
- Used in the pgbench expression parsing infrastructure to build function argument lists
- The head/tail pointer design enables efficient append operations during expression tree construction