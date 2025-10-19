# tas

## Location
[src/include/storage/s_lock.h:485-509](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/s_lock.h#L485-L509)

## Overview
The  function implements Test-And-Set atomic operation for 32-bit i386 architecture, providing the low-level mechanism for spinlock synchronization in PostgreSQL.

## Definition

```c
struction */
#define S_UNLOCK(lock)	\
do \
{ \
	__asm__ __volatile__( \
		"       .set push           \n" \
		MIPS_SET_MIPS2 \
		"       .set noreorder      \n" \
		"       .set nomacro        \n" \
		"       sync                \n" \
		"       .set pop              " \
:		/* no outputs */ \
:		/* no inputs */	\
:		"memory"); \
	*((volatile slock_t *) (lock)) = 0; \
} while (0)

#endif /* __mips__ && !__sgi */


#if defined(__hppa) || defined(__hppa__)	/* HP PA-RISC */
/*
 * HP's PA-RISC
 *
 * Because LDCWX requires a 16-byte-aligned address, we declare slock_t as a
 * 16-byte struct.  The active word in the struct is whichever has the aligned
 * address;
```
## Detailed Description
The  function is a platform-specific implementation of the Test-And-Set operation for 32-bit i386 processors. It uses inline assembly to perform an atomic exchange operation that is fundamental to PostgreSQL's spinlock mechanism. The function first performs a non-locking test to check if the lock is already taken, and only if it appears free does it attempt the actual atomic exchange using the  instruction with a  prefix.

The implementation includes a performance optimization where it checks the lock value before attempting the atomic operation. This reduces bus contention on multiprocessor systems, though the comments indicate the performance benefits vary across different x86 platforms.

The function uses GCC inline assembly with specific constraints to ensure proper memory ordering and prevent compiler optimizations that could break the atomicity guarantees.

## Parameters / Member Variables
- : A pointer to a volatile slock_t (unsigned char) that represents the spinlock to be acquired

## Dependencies
- Functions called/Symbols referenced:
  - [slock_t](../s/slock_t.md) (typedef for unsigned char)
- Called from (representative examples):
  - TAS macro (defined as TAS(lock) tas(lock))
  - Used indirectly through SPIN_DELAY macro

## Notes and Other Information
- This implementation is specific to 32-bit i386 architecture (#ifdef __i386__)
- The function returns 1 if the lock was already taken, 0 if successfully acquired
- Uses inline assembly with 'lock' prefix to ensure atomicity across CPU cores
- The non-locking test optimization may be better suited for TAS_SPIN() rather than TAS() in modern code
- Memory barrier ("memory") and condition codes ("cc") are marked as clobbered
- Part of PostgreSQL's platform-specific spinlock implementation in s_lock.h

## Simplified Source

```c
static __inline__ int
tas(volatile slock_t *lock)
{
    slock_t result = 1;

    // Performance optimization: check if lock is free before atomic operation
    // Only attempt atomic exchange if lock appears available
    __asm__ __volatile__(
        "    cmpb    $0,%1    \n"     // Compare lock value with 0
        "    jne     1f       \n"     // Jump if not equal (lock taken)
        "    lock             \n"     // Memory barrier for atomicity
        "    xchgb   %0,%1    \n"     // Atomic exchange: swap result with lock
        "1:                   \n"     // Label for early exit
        : "+q"(result), "+m"(*lock)   // Input/output constraints
        :                             // No additional inputs
        : "memory", "cc");            // Clobbered: memory and condition codes

    return (int) result;  // Returns 1 if lock was taken, 0 if acquired
}
```