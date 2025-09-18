# pg_attribute_no_sanitize_alignment

## Location
src/port/pg_crc32c_sse42.c: 21 - 69

## Overview
A compiler attribute macro that disables alignment sanitization for functions that intentionally perform unaligned memory accesses.

## Definition

Located in .

## Detailed Description
This macro is used to annotate functions that intentionally perform unaligned memory accesses, telling the compiler's address sanitizer to skip alignment checking for those functions. It expands to the GCC/Clang  attribute on supported compiler versions (Clang 7+ or GCC 8+), and to nothing on older compilers.

The macro is specifically designed for performance-critical code on architectures like x86/x86_64 that can handle unaligned accesses efficiently. It prevents false positives when using alignment sanitizers during testing while allowing the code to make intentional unaligned memory accesses for performance reasons.

## Parameters / Member Variables
This is a parameterless macro that expands to either:
-  on supported compilers
- An empty string on unsupported compilers

## Dependencies
- Functions annotated with this attribute:
  -  in 
- Called from:
  - Applied as a function attribute decorator

## Notes and Other Information
- Should be used carefully and primarily on x86-specific code where unaligned accesses are known to be safe and performant
- Enables testing with alignment sanitizers () without false positives
- The macro is conditionally compiled based on compiler version support
- Used in performance-critical CRC32C computation code that deliberately performs unaligned memory reads for efficiency
- Comments in the source indicate that performance testing showed no benefit from aligning memory access patterns, justifying the use of unaligned accesses