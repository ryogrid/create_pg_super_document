# eat

## Location
[src/timezone/zic.c:482-487](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L482-L487)

## Overview
A simplified wrapper function for setting single-file error context in the PostgreSQL timezone compiler.

## Definition
```c
static void
eat(char const *name, lineno_t num)
```

## Detailed Description
The `eat` function is a convenience wrapper around the more comprehensive `eats` function, designed for cases where only single-file error context is needed. It calls `eats` with NULL and -1 for the rule file context parameters, effectively setting up error reporting for situations where no rule file context is relevant.

This function provides a simpler interface for the common case of setting error context when processing a single file, while still leveraging the full error context infrastructure provided by the `eats` function.

## Parameters / Member Variables
- `name`: The filename where processing is occurring
- `num`: The line number in the file

## Dependencies
- Functions called/Symbols referenced:
  - lineno_t (type)
  - [eats](eats.md)

- Called from (representative examples):
  - [main](../m/main.md) (multiple calls)
  - [associate](../a/associate.md) (multiple calls)
  - [infile](../i/infile.md)
  - [years_of_observations](../y/years_of_observations.md) (multiple calls)

## Notes and Other Information
- This function is static, meaning it's only accessible within src/timezone/zic.c
- Provides a simplified interface to the dual-context error reporting system
- Commonly used throughout the codebase for single-file error context setup
- Always sets rule context to NULL/-1, indicating no rule file context
- Part of the comprehensive error handling infrastructure in the timezone compiler
- Used extensively in main processing loops and file parsing operations

## Simplified Source

```c
static void eat(char const *name, lineno_t num) {
    eats(name, num, NULL, -1);
}
```