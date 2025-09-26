# ecpyalloc

## Location
[src/timezone/zic.c:446-451](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L446-L451)

## Overview
A utility function in the PostgreSQL timezone compiler (zic) that performs error-checked string duplication.

## Definition

```c
enum = num;
```
## Detailed Description
The  function is a wrapper around the standard  function that provides memory allocation checking. It duplicates the input string and verifies that the memory allocation was successful through the  function. This is part of the timezone compiler's robust memory management strategy, ensuring that string duplication operations don't silently fail due to memory exhaustion.

The function name suggests "error-checked copy allocation," reflecting its role as a safe alternative to direct  calls throughout the zic codebase.

## Parameters / Member Variables
- : A constant string pointer to be duplicated

## Dependencies
- Functions called/Symbols referenced:
  - [memcheck](../m/memcheck.md)
  - strdup (implicit via memcheck)

- Called from (representative examples):
  - [inrule](../i/inrule.md) (multiple calls)
  - [inzsub](../i/inzsub.md) (multiple calls) 
  - [inlink](../i/inlink.md) (multiple calls)
  - [rulesub](../r/rulesub.md) (multiple calls)
  - [mkdirs](../m/mkdirs.md)

## Notes and Other Information
- This function is static, meaning it's only accessible within the src/timezone/zic.c file
- The function serves as a centralized point for string duplication with error checking
- It's extensively used throughout the timezone rule parsing and processing logic
- The memcheck wrapper ensures the program terminates gracefully if memory allocation fails