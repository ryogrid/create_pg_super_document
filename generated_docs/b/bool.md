# bool

## Location
src/backend/utils/fmgr/dfmgr.c: 28 - 48

## Overview
The `bool` type is PostgreSQL's fundamental boolean data type that can hold either true or false values, providing a standardized way to represent boolean logic throughout the codebase.

## Definition
```c
typedef unsigned char bool;
#define true	((bool) 1)
#define false	((bool) 0)
```

## Detailed Description
PostgreSQL defines its own `bool` type to ensure consistent boolean representation across all platforms and compiler environments. The implementation prioritizes compatibility and performance:

- **Conditional inclusion**: Uses C99's `<stdbool.h>` when `PG_USE_STDBOOL` is defined and the standard bool has size 1, otherwise falls back to a custom typedef
- **Size guarantee**: Always ensures bool is exactly 1 byte (unsigned char), which is critical for PostgreSQL's storage and serialization mechanisms
- **Cross-platform consistency**: Provides uniform boolean behavior regardless of compiler or platform differences
- **C++ compatibility**: Relies on the compiler's built-in bool type when compiling as C++

The design philosophy emphasizes reliability and predictable behavior over potential compiler-specific optimizations, ensuring that boolean values behave consistently across PostgreSQL's entire codebase.

## Parameters / Member Variables
- `bool`: 1-byte unsigned integer type capable of holding 0 or 1
- `true`: Macro defined as `((bool) 1)` representing logical true
- `false`: Macro defined as `((bool) 0)` representing logical false

## Dependencies
- Functions called/Symbols referenced:
  - None (fundamental type definition)
- Called from (representative examples):
  - Used extensively throughout PostgreSQL codebase in function signatures, variables, and expressions
  - Critical for all boolean logic, conditional statements, and flag management

## Notes and Other Information
- **Static assertions**: PostgreSQL includes compile-time checks to ensure bool remains 1 byte in size
- **ECPG compatibility**: A parallel definition exists in `src/interfaces/ecpg/include/ecpglib.h` for embedded SQL
- **Storage implications**: The 1-byte size is crucial for on-disk storage formats and network protocol compatibility
- **Debugging benefits**: Using standard bool when available improves debugger output and third-party library compatibility
- **Historical context**: This approach predates widespread C99 adoption and maintains backward compatibility with older compilers