# reduce

## Location
[src/tools/pg_bsd_indent/parse.c:260-338](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/parse.c#L260-L338)

## Overview
The reduce function implements the reduction phase of the parsing algorithm in PostgreSQL's pg_bsd_indent tool, performing grammar reductions on parsed C code structures to simplify the parse stack.

## Definition


## Detailed Description
The reduce function is a core component of the pg_bsd_indent tool's parsing algorithm that implements a bottom-up parser reduction phase. It repeatedly applies grammar reduction rules to the parse stack until no more reductions are possible. The function operates on a stack-based parsing system where different C language constructs are recognized and reduced according to specific patterns.

The reduction rules handle various C language constructs including:
- Statement sequences (stmt + stmt → stmtl)
- Control flow structures (do-while, if-else, switch, for, while)
- Declarations
- Statement lists

Each reduction also manages indentation levels by setting ps.i_l_follow (indentation for the following line) based on the indentation level associated with the reduced construct.

## Parameters / Member Variables
- No parameters (operates on global parser state)

## Dependencies
- Functions called/Symbols referenced:
  - Uses various token types: stmt, stmtl, dolit, dohead, ifstmt, ifhead, swstmt, decl, elsehead, forstmt, whilestmt, lbrace
  - Accesses global parser state through ps structure (ps.p_stack, ps.tos, ps.i_l_follow, ps.il, ps.cstk)
  - References case_ind for switch statement indentation handling
- Called from (representative examples):
  - [parse](../p/parse.md): The main parsing function in pg_bsd_indent calls reduce during parsing
  - [startScan](../s/startScan.md): Called from GIN index scanning functions (different context)

## Notes and Other Information
- The function implements a classic LR-style parser reduction phase with specific rules for C language constructs
- Contains detailed algorithm documentation from original 1976 implementation by D A Willcox
- Handles complex indentation logic for nested control structures
- The reduction rules are specifically tailored for C code formatting and indentation
- Uses a switch-case structure with nested logic to handle different token combinations on the parse stack
- Part of PostgreSQL's pg_bsd_indent tool for code formatting consistency
- The function includes fallthrough logic for similar statement types that require identical handling