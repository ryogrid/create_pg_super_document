# illegal_instruction_handler

## Location
[src/port/pg_crc32c_armv8_choose.c:40-45](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pg_crc32c_armv8_choose.c#L40-L45)

## Overview
A static signal handler function used to detect ARMv8 CRC instruction availability by catching SIGILL (illegal instruction) signals during runtime probing.

## Definition

```c
static void
illegal_instruction_handler(SIGNAL_ARGS)
```
## Detailed Description
This function serves as a signal handler specifically designed to catch SIGILL signals that occur when attempting to execute ARMv8 CRC instructions on hardware that doesn't support them. It's part of PostgreSQL's runtime CPU feature detection mechanism for ARMv8 CRC-32C acceleration.

When PostgreSQL needs to determine if the current ARM processor supports hardware CRC-32C instructions, it attempts to execute such an instruction within a controlled environment. If the processor lacks this capability, the CPU will raise a SIGILL signal. This handler catches that signal and uses siglongjmp() to return control to the detection code, allowing PostgreSQL to gracefully fall back to software-based CRC computation.

The function works in conjunction with a sigsetjmp/siglongjmp mechanism to implement a "try-catch" style error handling for illegal instructions, which is not natively supported in C.

## Parameters / Member Variables
- Uses  macro which typically expands to  for signal number parameter

## Dependencies
- Functions called/Symbols referenced:
  -  - performs non-local jump to saved context
  -  - macro defining signal handler parameter signature
  -  - static sigjmp_buf for jump target
- Called from (representative examples):
  -  - sets this as SIGILL handler during CPU feature detection

## Notes and Other Information
- This is a static function, only visible within pg_crc32c_armv8_choose.c
- Part of PostgreSQL's hardware acceleration detection for ARM processors
- Uses setjmp/longjmp mechanism which is generally discouraged but necessary here for signal handling
- The handler must be carefully managed to avoid interfering with normal program operation
- Only active during the brief window of CPU feature detection, then restored to default handling