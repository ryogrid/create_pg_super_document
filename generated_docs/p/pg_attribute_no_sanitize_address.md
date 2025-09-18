# pg_attribute_no_sanitize_address

## Location
src/include/common/hashfn_unstable.h: 251 - 288

## Overview
A preprocessor macro that provides compiler attributes to disable AddressSanitizer checks for specific functions, allowing them to bypass memory access validation during sanitized builds.

## Definition


## Detailed Description
This macro is a platform-specific abstraction for disabling AddressSanitizer (ASan) checks on individual functions. AddressSanitizer is a runtime memory error detector that can catch buffer overflows, use-after-free bugs, and other memory access violations. However, there are legitimate cases where low-level code needs to perform operations that would trigger false positives or where the overhead of sanitization is unacceptable.

The macro provides conditional compilation support:
- For GCC 8+ and Clang: Uses the modern  attribute
- For older Clang versions with : Uses the deprecated  attribute  
- For unsupported compilers: Expands to nothing (no-op)

This design ensures compatibility across different compiler versions while providing the sanitizer control where supported.

## Parameters / Member Variables
This is a parameterless macro that expands to the appropriate compiler attribute or nothing.

## Dependencies
- Functions called/Symbols referenced:
  -  (compiler builtin for feature detection)
- Called from (representative examples):
  - Currently no direct references found in the analyzed codebase

## Notes and Other Information
- The comment in the source warns "Think twice before using this!" indicating it should be used sparingly and only when absolutely necessary
- Primarily intended for low-level memory manipulation code where sanitizer checks would be counterproductive
- Part of PostgreSQL's compiler compatibility abstraction layer in 
- Should only be used when the developer is confident the code is memory-safe despite triggering sanitizer warnings
- Related to other sanitizer control macros like 