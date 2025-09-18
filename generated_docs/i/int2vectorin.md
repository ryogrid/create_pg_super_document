# int2vectorin

## Location
[src/backend/utils/adt/int.c:141-206](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L141-L206)

## Overview
Converts a string representation of space-separated smallint values ("num num ...") into the internal PostgreSQL int2vector data type.

## Definition


## Detailed Description
This function parses a textual representation of a vector of smallint (int2) values and converts it into PostgreSQL's internal int2vector format. The input string should contain space-separated integer values within the smallint range (-32768 to 32767). The function dynamically allocates memory for the result vector, starting with an initial guess of 32 elements and doubling the allocation when needed. It performs comprehensive error checking for invalid syntax, out-of-range values, and improper formatting.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : Input C-string containing space-separated smallint values
  - : Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  -  (data type)
  -  (macro for size calculation)
  -  (memory allocation)
  -  (memory reallocation)
  -  (error return with context)
  -  (set variable size)
  -  (string to long conversion)
- Called from (representative examples):
  - PostgreSQL type input/output system
  - SQL parsing and execution engine

## Notes and Other Information
- The function starts with an arbitrary initial allocation of 32 elements and doubles when needed for efficiency
- Supports soft error handling through the escontext parameter
- Sets standard array metadata: ndim=1, dataoffset=0, elemtype=INT2OID, lbound1=0
- Performs strict validation of input format and numeric ranges
- Returns a properly formatted int2vector suitable for internal PostgreSQL operations