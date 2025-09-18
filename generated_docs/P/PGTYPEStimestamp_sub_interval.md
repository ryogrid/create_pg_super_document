# PGTYPEStimestamp_sub_interval

## Location
[src/interfaces/ecpg/pgtypeslib/timestamp.c:917-925](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/timestamp.c#L917-L925)

## Overview
Subtracts an interval from a PostgreSQL timestamp by negating the interval components and delegating to the addition function.

## Definition
```c
int PGTYPEStimestamp_sub_interval(timestamp *tin, interval *span, timestamp *tout)
```

## Detailed Description
This function implements timestamp-interval subtraction using a simple but effective approach: it creates a temporary interval with negated month and time components, then calls PGTYPEStimestamp_add_interval to perform the actual arithmetic. This design leverages the existing complex logic in the addition function for handling month boundaries, leap years, and other calendar arithmetic complexities, avoiding code duplication while ensuring consistent behavior between addition and subtraction operations.

## Parameters / Member Variables
- `tin`: Pointer to the input timestamp from which the interval will be subtracted
- `span`: Pointer to the interval to be subtracted from the timestamp  
- `tout`: Pointer to the output timestamp where the result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [PGTYPEStimestamp_add_interval](PGTYPEStimestamp_add_interval.md)
- Called from (representative examples):
  - (Referenced in header file but no direct callers found in codebase)

## Notes and Other Information
- Returns 0 on success, -1 on failure (inherited from PGTYPEStimestamp_add_interval)
- Implements subtraction by negating interval components and using addition logic
- Inherits all the complex calendar arithmetic handling from the addition function
- Ensures consistent behavior between timestamp addition and subtraction operations
- Part of the ECPG pgtypes library for embedded SQL applications