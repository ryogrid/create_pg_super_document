# oidvectorin

## Location
[src/backend/utils/adt/oid.c:114-157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oid.c#L114-L157)

## Overview
Input function that converts a string representation of space-separated OIDs ("num num ...") into PostgreSQL's internal oidvector format.

## Definition


## Detailed Description
The  function is a type input function that parses a string containing whitespace-separated OID values and converts them into PostgreSQL's internal oidvector data structure. This function is part of PostgreSQL's type system infrastructure, specifically handling the conversion from external string representation to internal binary format for oidvector types.

The function dynamically allocates memory for the result, starting with an initial guess of 32 elements and doubling the allocation as needed. It uses  to parse individual OID values and properly handles error conditions through the soft error mechanism. The resulting oidvector is properly initialized with all required metadata including dimensions, bounds, and data type information.

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention macro that provides:
  - : Input C-string containing space-separated OID values
  - : Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - oidvector (data type)
  - OidVectorSize (macro for calculating oidvector size)
  - [palloc0](../p/palloc0.md) (memory allocation with zero initialization)
  - [repalloc](../r/repalloc.md) (memory reallocation)
  - [uint32in_subr](../u/uint32in_subr.md) (OID parsing function)
  - SOFT_ERROR_OCCURRED (error checking macro)
  - SET_VARSIZE (macro to set variable-length type size)
- Called from (representative examples):
  - PostgreSQL type system during input conversion
  - SQL parsing and execution engine

## Notes and Other Information
- Starts with an arbitrary initial allocation of 32 OIDs and dynamically expands as needed
- Properly handles whitespace separation between OID values
- Uses soft error handling mechanism to report parsing errors gracefully
- Sets all required oidvector metadata including ndim=1, dataoffset=0, elemtype=OIDOID, and proper bounds
- Memory allocation follows PostgreSQL's palloc/repalloc pattern for automatic cleanup