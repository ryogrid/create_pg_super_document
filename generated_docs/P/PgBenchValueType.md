# PgBenchValueType

## Location
[src/bin/pgbench/pgbench.h:42-53](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.h#L42-L53)

## Overview
PgBenchValueType is an enumeration that defines the possible data types for values used in pgbench expressions and variables. It serves as a type discriminator in the PostgreSQL benchmark tool to distinguish between different kinds of values that can be stored and manipulated during benchmark execution.

## Definition
```c
typedef enum
{
    PGBT_NO_VALUE = 0,
    PGBT_NULL,
    PGBT_INT,
    PGBT_DOUBLE,
    PGBT_BOOLEAN,
    /* add other types here */
} PgBenchValueType;
```

## Detailed Description
PgBenchValueType is a fundamental type system component in pgbench that enables type-safe operations on values within benchmark scripts. It works in conjunction with the PgBenchValue structure to create a tagged union system where each value carries both its data and type information. This design allows pgbench to support multiple data types (integers, doubles, booleans, and nulls) while maintaining type safety and enabling appropriate type coercion when needed.

The enumeration starts with PGBT_NO_VALUE as 0 to represent uninitialized or empty values, followed by SQL-like types including NULL, integer, floating-point, and boolean values. The type system is designed to be extensible, as indicated by the comment allowing for additional types.

## Parameters / Member Variables
- `PGBT_NO_VALUE`: Represents an uninitialized or empty value (default state)
- `PGBT_NULL`: Represents a SQL NULL value
- `PGBT_INT`: Represents a 64-bit signed integer value 
- `PGBT_DOUBLE`: Represents a double-precision floating-point value
- `PGBT_BOOLEAN`: Represents a boolean true/false value

## Dependencies
- Functions called/Symbols referenced:
  - Used as a component in PgBenchValue struct
- Called from (representative examples):
  - Expression parser in exprparse.y
  - Value manipulation functions throughout pgbench.c
  - [Variable](../V/Variable.md) assignment and retrieval functions

## Notes and Other Information
- The enum is designed to work with a union in PgBenchValue struct, enabling efficient storage of different data types
- Type checking and coercion logic throughout pgbench.c relies heavily on these type constants
- The NO_VALUE state is used to detect uninitialized variables and trigger appropriate error handling
- The type system supports SQL-like semantics including proper NULL handling in expressions
- Future extensibility is planned through the "add other types here" comment