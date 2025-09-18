# SPI_is_cursor_plan

## Location
[src/backend/executor/spi.c:1910-1947](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L1910-L1947)

## Overview
Determine whether a prepared SPI plan can be used to open a cursor by checking if it contains exactly one command that returns tuples.

## Definition


## Detailed Description
SPI_is_cursor_plan examines a prepared SPI plan to determine if it is suitable for cursor operations. A plan is considered cursor-compatible if it contains exactly one command and that command returns tuples to the caller (such as SELECT or INSERT ... RETURNING, but not SELECT ... INTO). This function is essential for validating plans before attempting to use them with SPI_cursor_open.

The function performs several checks:
1. Validates the plan pointer and magic number
2. Ensures the plan contains exactly one cached plan source (one pre-rewrite command)
3. Checks if the plan source has a result descriptor, indicating it returns tuples

The function does not force revalidation of the cached plan, as invalidation typically affects the rowtype of returned tuples rather than whether tuples are returned at all.

## Parameters / Member Variables
- : An SPIPlanPtr pointing to a previously prepared SPI plan using SPI_prepare. Must be a valid, non-NULL plan.

## Dependencies
- Functions called/Symbols referenced:
  - [SPIPlanPtr](SPIPlanPtr.md) (typedef for struct _SPI_plan *)
  - CachedPlanSource (struct representing cached plan information)
  - _SPI_PLAN_MAGIC (validation constant)
  - SPI_ERROR_ARGUMENT (error code)
  - list_length (list utility function)
  - linitial (list utility function)
- Called from (representative examples):
  - [SPI_cursor_open_internal](SPI_cursor_open_internal.md) (internal cursor opening function)
  - User code validating plans before cursor operations

## Notes and Other Information
- Returns false and sets SPI_result to SPI_ERROR_ARGUMENT if the plan is invalid or NULL
- Returns false and sets SPI_result to 0 if the plan contains multiple commands or no commands
- Returns true only if the plan contains exactly one command that returns tuples
- The function checks the resultDesc field of the CachedPlanSource to determine if tuples are returned
- Commands like SELECT, INSERT ... RETURNING return tuples, while SELECT ... INTO does not
- Essential for preventing runtime errors when attempting cursor operations on incompatible plans
- Used internally by SPI cursor functions to validate plan compatibility
- The function does not modify the plan and is safe to call multiple times