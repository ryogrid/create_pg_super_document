# PGTYPESdecimal_new

## Location
[src/interfaces/ecpg/pgtypeslib/numeric.c:59-77](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/numeric.c#L59-L77)

## Overview
A constructor function that creates and initializes a new decimal value structure for use with PostgreSQL's ECPG (Embedded SQL in C) pgtypes library, specifically for Informix compatibility.

## Definition
```c
decimal *
PGTYPESdecimal_new(void)
```

## Detailed Description
The `PGTYPESdecimal_new` function serves as the primary constructor for creating new decimal values in PostgreSQL's ECPG pgtypes library. This function is specifically designed to provide Informix compatibility within the PostgreSQL ecosystem. Unlike the more complex numeric type, the decimal type uses a simpler fixed-size structure that can be completely initialized with a simple memory clear operation. The function allocates memory for the decimal structure and ensures it's properly zeroed out, providing a clean initial state ready for value assignment.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [pgtypes_alloc](../p/pgtypes_alloc.md) (allocates memory for the decimal structure)
  - memset (zeros out the allocated structure)
- Called from (representative examples):
  - Various test programs (dec_test.c, num_test2.c)
  - ECPG precompiler generated code
  - SQL descriptor area handling (sqlda.c)

## Notes and Other Information
- Returns a pointer to a newly allocated decimal structure, or NULL if allocation fails
- The decimal structure is completely zeroed out using memset, ensuring all fields start with known values
- This function is part of the Informix compatibility layer within PostgreSQL's ECPG system
- The decimal type is simpler than the numeric type and doesn't require complex digit buffer allocation
- Proper error handling ensures NULL is returned if memory allocation fails
- The returned decimal should be freed using appropriate cleanup functions when no longer needed
- This function is primarily used in test code and generated ECPG applications that require Informix-style decimal handling