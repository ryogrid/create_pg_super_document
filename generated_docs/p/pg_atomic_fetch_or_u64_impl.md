# pg_atomic_fetch_or_u64_impl

## Location
src/include/port/atomics/generic.h: 393 - 403

## Overview
Performs an atomic fetch-and-OR operation on a 64-bit unsigned integer, returning the original value before the OR operation was applied.

## Definition


## Detailed Description
This function implements the low-level atomic fetch-and-OR operation for 64-bit unsigned integers using GCC's built-in  intrinsic. It atomically performs a bitwise OR operation between the value stored at the memory location pointed to by  and the value , returning the original value that was stored before the operation. This is a fundamental building block for PostgreSQL's atomic operations framework, providing lock-free synchronization primitives.

The function is marked as  for performance, allowing the compiler to inline the call and optimize it directly to the underlying hardware atomic instruction when possible.

## Parameters / Member Variables
- : Pointer to the atomic 64-bit unsigned integer variable to be modified
- : The value to be bitwise ORed with the current value

## Dependencies
- Functions called/Symbols referenced:
  -  (GCC builtin)
  -  (type)
- Called from (representative examples):
  - 

## Notes and Other Information
- This is a GCC-specific implementation located in the generic-gcc.h header
- The function uses GCC's legacy  builtin functions for atomic operations
- The  qualifier ensures that the compiler doesn't optimize away memory accesses
- This implementation is used when more modern atomic intrinsics are not available
- The function is thread-safe and provides memory ordering guarantees as defined by the underlying GCC builtin