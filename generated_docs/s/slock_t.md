# slock_t

## Location
[src/include/storage/s_lock.h:735-741](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/s_lock.h#L735-L741)

## Overview
 is a platform-dependent typedef that defines the fundamental data type used for PostgreSQL's spinlock implementation across different hardware architectures.

## Definition

```c
typedef int slock_t;
```
## Detailed Description
 serves as the hardware-abstraction layer for spinlocks in PostgreSQL. The actual type definition varies by platform to optimize for the underlying hardware's atomic operation capabilities. This typedef is defined in  with different implementations selected based on compile-time platform detection.

The type is specifically designed to work with test-and-set atomic operations that are fundamental to spinlock implementation. Different platforms require different data types based on their atomic instruction sets:

- **x86/x86_64**: Uses  for byte-level atomic operations with XCHG instruction
- **SPARC**: Uses  for word-level atomic operations  
- **ARM**: Uses  for byte-level atomic operations
- **Windows**: Uses  to interface with Windows API atomic functions
- **Fallback**: Uses  when hardware-specific implementations are unavailable

This abstraction allows PostgreSQL to provide consistent spinlock semantics across all supported platforms while leveraging the most efficient atomic operations available on each architecture.

## Parameters / Member Variables
This is a typedef, not a struct, so it has no members. However, the underlying type characteristics are:
- **Size**: Varies by platform (1, 4, or 8 bytes depending on typedef)
- **Alignment**: Platform-specific alignment requirements for atomic operations
- **Value semantics**: 0 typically indicates unlocked, non-zero indicates locked state

## Dependencies
- Functions that operate on slock_t:
  - 
  -   
  - 
  - 
  - 
  - 
  -  (test-and-set)

- Used extensively in (representative examples):
  -  - BRIN index shared state
  -  - WAL control structures
  -  - Logical replication slots
  -  - Shared memory message queues
  -  - [Hash](../H/Hash.md) table segments
  -  - Synchronization barriers
  - All PostgreSQL shared memory data structures requiring synchronization

## Notes and Other Information
- **Critical constraint**: Spinlocks must be held for only a few instructions to avoid system performance degradation
- **Memory ordering**: Operations include compiler barriers to prevent instruction reordering
- **Platform detection**: The appropriate typedef is selected automatically at compile time based on preprocessor macros
- **Interrupt safety**: Spinlock operations assume  cannot occur while lock is held
- **Volatility**: Callers should use  when passing to spinlock functions
- **Historical note**: Before PostgreSQL 9.5, callers needed volatile qualifiers to access spinlock-protected data
- **Implementation files**: Hardware-specific implementations in , high-level API in 
- **Testing**: Test infrastructure exists in  with 