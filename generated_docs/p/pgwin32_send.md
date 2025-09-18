# pgwin32_send

## Location
[src/backend/port/win32/socket.c:459-516](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/win32/socket.c#L459-L516)

## Overview
Windows-specific socket send function that provides PostgreSQL-compatible socket sending functionality with signal handling support on Windows platforms.

## Definition


## Detailed Description
 is a Windows-specific wrapper around the Windows Socket API  function that provides PostgreSQL-compatible socket sending behavior. The function implements proper signal handling during socket operations and handles blocking/non-blocking modes appropriately for PostgreSQL's needs on Windows.

The function uses  instead of the standard  to ensure proper integration with Windows' asynchronous socket model while maintaining compatibility with PostgreSQL's socket handling expectations. It includes a retry loop to handle cases where UDP sockets may become busy again after initially appearing ready.

Key features:
- Integrates with PostgreSQL's signal handling system via 
- Supports both blocking and non-blocking socket modes
- Handles Windows-specific socket error conditions
- Uses  for improved Windows socket performance
- Implements retry logic for UDP socket readiness edge cases

## Parameters / Member Variables
- : The Windows socket descriptor ( type) to send data through
- : Pointer to the buffer containing data to send (const void* for type safety)
- : Number of bytes to send from the buffer
- : Socket send flags passed through to 

## Dependencies
- Functions called/Symbols referenced:
  - : Check for and handle pending PostgreSQL signals
  - : Windows Socket API function for sending data
  - : Get Windows socket error codes
  - : Convert Windows socket errors to PostgreSQL errno values
  - : Wait for socket to become ready for specified operations
  - : POSIX error code for non-blocking operation that would block
- Called from (representative examples):
  - No direct references found in the current codebase (likely used via function pointer or macro substitution)

## Notes and Other Information
- Windows-only function (part of )
- Function signature is designed to match POSIX  for compatibility
- The  parameter is cast to  when used with  structure due to Windows API requirements
- Includes special handling for PostgreSQL's "emulated non-blocking mode" ()
- Part of PostgreSQL's Windows socket abstraction layer
- Contains retry loop specifically for UDP socket edge cases where readiness may be transient
- Error handling includes both Windows socket errors and PostgreSQL signal interruptions