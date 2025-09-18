# SPI_inside_nonatomic_context

## Location
src/backend/executor/spi.c: 581 - 595

## Overview
SPI_inside_nonatomic_context determines whether the current execution is inside a procedure (nonatomic SPI context) rather than a function context.

## Definition
```c
bool SPI_inside_nonatomic_context(void)
```

## Detailed Description
This function checks if the current SPI execution context is nonatomic, which typically means execution is happening inside a stored procedure rather than a function. The distinction is important because procedures can perform transaction control operations (COMMIT/ROLLBACK) while functions cannot.

The function performs three checks:
1. Verifies that there is an active SPI context
2. Checks if the current context is marked as nonatomic (atomic flag is false)
3. Ensures we're not within a subtransaction (which makes the context effectively atomic)

The logic matches the behavior of _SPI_commit regarding what constitutes an atomic vs nonatomic context.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - _SPI_current (global variable pointing to current SPI connection)
  - IsSubTransaction (function to check if in subtransaction)

- Called from (representative examples):
  - StartTransaction (src/backend/access/transam/xact.c:2140)

## Notes and Other Information
- Returns true only when in a nonatomic SPI context (procedure execution)
- Critical for transaction control decisions in stored procedures
- The checks must match _SPI_commit's logic for consistency
- Used by transaction management code to determine if transaction control is allowed
- Located in src/backend/executor/spi.c:581-595
- Part of PostgreSQL's stored procedure transaction control infrastructure