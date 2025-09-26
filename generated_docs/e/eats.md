# eats

## Location
[src/timezone/zic.c:473-481](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L473-L481)

## Overview
A function in the PostgreSQL timezone compiler that sets up error context information for enhanced error reporting.

## Definition
```c
static void
eats(char const *name, lineno_t num, char const *rname, lineno_t rnum)
```

## Detailed Description
The `eats` function is part of the error handling infrastructure in the zic timezone compiler. It sets up global variables that track both the primary and rule-related file context information for error reporting. This dual-context approach allows the compiler to provide more informative error messages by indicating both where an error occurred and which rule file context it relates to.

The function name likely stands for "error at this source" or similar, reflecting its role in establishing error location context. It's typically called before operations that might generate errors to ensure proper error reporting context.

## Parameters / Member Variables
- `name`: The current filename where processing is occurring
- `num`: The line number in the current file
- `rname`: The rule filename context (if applicable)
- `rnum`: The line number in the rule file context

## Dependencies
- Functions called/Symbols referenced:
  - lineno_t (type)

- Called from (representative examples):
  - [eat](eat.md)
  - [years_of_observations](../y/years_of_observations.md) (multiple calls)

## Notes and Other Information
- This function is static, meaning it's only accessible within src/timezone/zic.c
- Sets global variables (filename, linenum, rfilename, rlinenum) used by error reporting
- Part of the comprehensive error handling system in the timezone compiler
- The dual-context approach helps users understand both immediate and rule-related error sources
- Typically called before potentially error-prone operations to establish proper context
- Works in conjunction with the `eat` function for simpler single-context scenarios