# PgBenchFunction

## Location
[src/bin/pgbench/pgbench.h:104-105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.h#L104-L105)

## Overview
PgBenchFunction is an enumeration that defines all the built-in functions and operators available in pgbench expression language. It provides a comprehensive set of mathematical, logical, comparison, bitwise, and utility functions that can be used in benchmark scripts for data generation and manipulation.

## Definition
```c
typedef enum PgBenchFunction
{
    PGBENCH_ADD,
    PGBENCH_SUB,
    PGBENCH_MUL,
    PGBENCH_DIV,
    PGBENCH_MOD,
    PGBENCH_DEBUG,
    PGBENCH_ABS,
    PGBENCH_LEAST,
    PGBENCH_GREATEST,
    PGBENCH_INT,
    PGBENCH_DOUBLE,
    PGBENCH_PI,
    PGBENCH_SQRT,
    PGBENCH_LN,
    PGBENCH_EXP,
    PGBENCH_RANDOM,
    PGBENCH_RANDOM_GAUSSIAN,
    PGBENCH_RANDOM_EXPONENTIAL,
    PGBENCH_RANDOM_ZIPFIAN,
    PGBENCH_POW,
    PGBENCH_AND,
    PGBENCH_OR,
    PGBENCH_NOT,
    PGBENCH_BITAND,
    PGBENCH_BITOR,
    PGBENCH_BITXOR,
    PGBENCH_LSHIFT,
    PGBENCH_RSHIFT,
    PGBENCH_EQ,
    PGBENCH_NE,
    PGBENCH_LE,
    PGBENCH_LT,
    PGBENCH_IS,
    PGBENCH_CASE,
    PGBENCH_HASH_FNV1A,
    PGBENCH_HASH_MURMUR2,
    PGBENCH_PERMUTE,
} PgBenchFunction;
```

## Detailed Description
PgBenchFunction serves as the function identifier system for pgbenchs expression evaluation engine. Each enumeration value represents a specific built-in function or operator that can be called within pgbench expressions. The functions are categorized into several groups: arithmetic operations (ADD, SUB, MUL, DIV, MOD), mathematical functions (ABS, SQRT, LN, EXP, POW, PI), logical operations (AND, OR, NOT), bitwise operations (BITAND, BITOR, BITXOR, LSHIFT, RSHIFT), comparison operations (EQ, NE, LE, LT), random number generation (RANDOM, RANDOM_GAUSSIAN, RANDOM_EXPONENTIAL, RANDOM_ZIPFIAN), hashing functions (HASH_FNV1A, HASH_MURMUR2), utility functions (DEBUG, LEAST, GREATEST, INT, DOUBLE, IS, CASE), and data transformation (PERMUTE).

This enumeration is used throughout the expression evaluation system to identify which function to execute and to implement function-specific logic including argument validation, type coercion, and result computation.

## Parameters / Member Variables
- `PGBENCH_ADD`: Addition operator (+)
- `PGBENCH_SUB`: Subtraction operator (-)
- `PGBENCH_MUL`: Multiplication operator (*)
- `PGBENCH_DIV`: Division operator (/)
- `PGBENCH_MOD`: Modulo operator (%)
- `PGBENCH_DEBUG`: Debug output function
- `PGBENCH_ABS`: Absolute value function
- `PGBENCH_LEAST`: Return minimum of arguments
- `PGBENCH_GREATEST`: Return maximum of arguments
- `PGBENCH_INT`: Type conversion to integer
- `PGBENCH_DOUBLE`: Type conversion to double
- `PGBENCH_PI`: Pi constant function
- `PGBENCH_SQRT`: Square root function
- `PGBENCH_LN`: Natural logarithm function
- `PGBENCH_EXP`: Exponential function
- `PGBENCH_RANDOM`: Uniform random number generation
- `PGBENCH_RANDOM_GAUSSIAN`: Gaussian random number generation
- `PGBENCH_RANDOM_EXPONENTIAL`: Exponential random number generation
- `PGBENCH_RANDOM_ZIPFIAN`: Zipfian random number generation
- `PGBENCH_POW`: Power function
- `PGBENCH_AND`: Logical AND operator
- `PGBENCH_OR`: Logical OR operator
- `PGBENCH_NOT`: Logical NOT operator
- `PGBENCH_BITAND`: Bitwise AND operator
- `PGBENCH_BITOR`: Bitwise OR operator
- `PGBENCH_BITXOR`: Bitwise XOR operator
- `PGBENCH_LSHIFT`: Left bit shift operator
- `PGBENCH_RSHIFT`: Right bit shift operator
- `PGBENCH_EQ`: Equality comparison operator
- `PGBENCH_NE`: Not equal comparison operator
- `PGBENCH_LE`: Less than or equal comparison operator
- `PGBENCH_LT`: Less than comparison operator
- `PGBENCH_IS`: SQL IS operator for NULL checking
- `PGBENCH_CASE`: CASE expression for conditional logic
- `PGBENCH_HASH_FNV1A`: FNV-1a hash function
- `PGBENCH_HASH_MURMUR2`: MurmurHash2 hash function
- `PGBENCH_PERMUTE`: Data permutation function

## Dependencies
- Functions called/Symbols referenced:
  - Used in PgBenchExpr struct for function expressions
  - Referenced by expression evaluation functions
- Called from (representative examples):
  - [isLazyFunc](../i/isLazyFunc.md)() in pgbench.c:2125
  - [evalLazyFunc](../e/evalLazyFunc.md)() in pgbench.c:2133
  - [evalStandardFunc](../e/evalStandardFunc.md)() in pgbench.c:2250
  - [evalFunc](../e/evalFunc.md)() in pgbench.c:2822

## Notes and Other Information
- The enumeration supports both eager and lazy evaluation functions, with some functions like CASE and logical operators supporting short-circuit evaluation
- Random functions support various statistical distributions important for realistic benchmark workloads
- [Hash](../H/Hash.md) functions provide data distribution capabilities for partitioning and load balancing scenarios  
- The function set is designed to support complex benchmark scenarios including mathematical computations, data generation, and conditional logic
- Type coercion and validation rules vary by function and are implemented in the evaluation functions