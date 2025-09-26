# pg_crc32c_armv8_available

## Location
src/port/pg_crc32c_armv8_choose.c: 46 - 84

## Overview
A static runtime detection function that determines whether the current ARM processor supports ARMv8 CRC Extension instructions for hardware-accelerated CRC-32C computation.

## Definition
```c
static bool pg_crc32c_armv8_available(void)
```

## Detailed Description
This function implements a sophisticated CPU capability detection mechanism specifically for ARMv8 CRC Extension instructions. Rather than relying on CPU feature flags (which may not be accessible or reliable), it uses a "try and catch" approach by actually attempting to execute the hardware CRC instruction.

The detection works by:
1. Setting up a custom SIGILL signal handler to catch illegal instruction exceptions
2. Using sigsetjmp to establish a recovery point
3. Attempting to execute both ARMv8 hardware CRC (`pg_comp_crc32c_armv8`) and software CRC (`pg_comp_crc32c_sb8`) on the same test data
4. Comparing the results to ensure hardware implementation correctness
5. Returning to software fallback if SIGILL occurs or results disagree

This approach ensures that PostgreSQL only uses hardware acceleration when it's both available and produces correct results, providing robust fallback behavior across different ARM processors and virtualization environments.

## Parameters / Member Variables
- No parameters (void function)
- Local variables:
  - `data`: Test data (value 42) used for CRC computation comparison
  - `result`: Integer storing detection result (1 for available, 0 for mismatch, -1 for unavailable)

## Dependencies
- Functions called/Symbols referenced:
  - `pqsignal` - PostgreSQL signal handler setup function
  - `illegal_instruction_handler` - Custom SIGILL handler for trap recovery
  - `sigsetjmp` - Establishes jump point for exception handling
  - `pg_comp_crc32c_armv8` - ARMv8 hardware CRC function being tested
  - `pg_comp_crc32c_sb8` - Software CRC implementation for comparison
  - `SIG_DFL` - Default signal handler restoration
  - `elog` - PostgreSQL logging function (backend builds only)
- Called from (representative examples):
  - `pg_comp_crc32c_choose` - Uses this for runtime implementation selection

## Notes and Other Information
- This is a static function, only visible within pg_crc32c_armv8_choose.c
- Uses signal handling which requires careful error state management
- Includes additional validation in backend builds to detect hardware/software result mismatches
- The function may only be called once during program initialization due to function pointer replacement mechanism
- Frontend and backend builds have slightly different behavior (frontend skips error logging)
- Critical for PostgreSQL performance on ARM platforms where CRC-32C is heavily used for data integrity