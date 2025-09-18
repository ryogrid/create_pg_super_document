# pg_re_throw

## Location
src/backend/utils/error/elog.c: 2001 - 2055

## Overview
pg_re_throw is the out-of-line implementation of the PG_RE_THROW() macro, responsible for propagating errors through PostgreSQL's exception handling mechanism using setjmp/longjmp.

## Definition


## Detailed Description
pg_re_throw implements the core logic for re-throwing errors in PostgreSQL's exception handling system. When called, it attempts to propagate the current error to the next outer setjmp handler using siglongjmp. If no outer handler exists (meaning an ERROR was thrown inside a PG_TRY block but there's no outer catch block), it promotes the error to FATAL level and processes it accordingly.

The function handles two scenarios:
1. Normal case: If PG_exception_stack is not NULL, it uses siglongjmp to jump to the next outer exception handler
2. No outer handler: If there's no outer setjmp handler, it promotes the ERROR to FATAL, recalculates output destinations, clears the error context stack, and calls errfinish to process the fatal error

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - siglongjmp
  - ErrorData (type)
  - [should_output_to_server](../s/should_output_to_server.md)
  - [should_output_to_client](../s/should_output_to_client.md)
  - [errfinish](../e/errfinish.md)
  - [ExceptionalCondition](../E/ExceptionalCondition.md)

- Called from (representative examples):
  - PG_RE_THROW (macro)

## Notes and Other Information
- This function never returns under normal circumstances
- When no outer exception handler exists, it automatically promotes ERROR to FATAL
- The error context stack is cleared when promoting to FATAL to avoid calling context routines twice
- Output destinations are recalculated when severity changes from ERROR to FATAL
- Uses ExceptionalCondition as a safety net in case the function unexpectedly attempts to return
- Part of PostgreSQL's setjmp/longjmp-based exception handling system