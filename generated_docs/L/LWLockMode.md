# LWLockMode

## Location
[src/include/storage/lwlock.h:119-176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/lwlock.h#L119-L176)

## Overview
LWLockMode is an enumeration that defines the different access modes available for PostgreSQL lightweight locks.

## Definition

```c
* to be used as LWLockAcquire argument */
} LWLockMode;


#ifdef LOCK_DEBUG
extern PGDLLIMPORT bool Trace_lwlocks;
#endif

extern bool LWLockAcquire(LWLock *lock, LWLockMode mode);
extern bool LWLockConditionalAcquire(LWLock *lock, LWLockMode mode);
extern bool LWLockAcquireOrWait(LWLock *lock, LWLockMode mode);
extern void LWLockRelease(LWLock *lock);
extern void LWLockReleaseClearVar(LWLock *lock, pg_atomic_uint64 *valptr, uint64 val);
extern void LWLockReleaseAll(void);
extern bool LWLockHeldByMe(LWLock *lock);
extern bool LWLockAnyHeldByMe(LWLock *lock, int nlocks, size_t stride);
extern bool LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode);

extern bool LWLockWaitForVar(LWLock *lock, pg_atomic_uint64 *valptr, uint64 oldval, uint64 *newval);
extern void LWLockUpdateVar(LWLock *lock, pg_atomic_uint64 *valptr, uint64 val);

extern Size LWLockShmemSize(void);
extern void CreateLWLocks(void);
extern void InitLWLockAccess(void);

extern const char *GetLWLockIdentifier(uint32 classId, uint16 eventId);

/*
 * Extensions (or core code) can obtain an LWLocks by calling
 * RequestNamedLWLockTranche() during postmaster startup.  Subsequently,
 * call GetNamedLWLockTranche() to obtain a pointer to an array containing
 * the number of LWLocks requested.
 */
extern void RequestNamedLWLockTranche(const char *tranche_name, int num_lwlocks);
extern LWLockPadded *GetNamedLWLockTranche(const char *tranche_name);

/*
 * There is another, more flexible method of obtaining lwlocks. First, call
 * LWLockNewTrancheId just once to obtain a tranche ID; this allocates from
 * a shared counter.  Next, each individual process using the tranche should
 * call LWLockRegisterTranche() to associate that tranche ID with a name.
 * Finally, LWLockInitialize should be called just once per lwlock, passing
 * the tranche ID as an argument.
 *
 * It may seem strange that each process using the tranche must register it
 * separately, but dynamic shared memory segments aren't guaranteed to be
 * mapped at the same address in all coordinating backends, so storing the
 * registration in the main shared memory segment wouldn't work for that case.
 */
extern int	LWLockNewTrancheId(void);
extern void LWLockRegisterTranche(int tranche_id, const char *tranche_name);
extern void LWLockInitialize(LWLock *lock, int tranche_id);

/*
 * Every tranche ID less than NUM_INDIVIDUAL_LWLOCKS is reserved; also,
 * we reserve additional tranche IDs for builtin tranches not included in
 * the set of individual LWLocks.  A call to LWLockNewTrancheId will never
 * return a value less than LWTRANCHE_FIRST_USER_DEFINED.
 */
typedef enum BuiltinTrancheIds
```
## Detailed Description
LWLockMode defines the three possible modes for lightweight lock operations in PostgreSQL. The enum provides the foundation for PostgreSQL's reader-writer locking semantics, allowing multiple concurrent readers or a single exclusive writer. The LW_WAIT_UNTIL_FREE mode is a special internal state used for process synchronization when waiting for any type of lock to be released, regardless of the intended access mode.

## Parameters / Member Variables
- `LW_EXCLUSIVE`: Exclusive lock mode - only one process can hold the lock, blocking all other access
- `LW_SHARED`: Shared lock mode - multiple processes can hold shared locks simultaneously, but blocks exclusive access
- `LW_WAIT_UNTIL_FREE`: Special internal mode used in PGPROC->lwWaitMode when waiting for a lock to become completely free

## Dependencies
- Functions called/Symbols referenced:
  - (None - enum definition)
- Called from (representative examples):
  - LWLockAcquire (acquiring locks in specified mode)
  - LWLockConditionalAcquire (attempting non-blocking lock acquisition)
  - LWLockHeldByMeInMode (checking lock ownership in specific mode)
  - LWLockAttemptLock (internal lock acquisition attempts)

## Notes and Other Information
- LW_EXCLUSIVE and LW_SHARED implement standard reader-writer lock semantics
- Multiple shared locks can be held simultaneously by different processes
- Exclusive locks are mutually exclusive with both shared and other exclusive locks
- LW_WAIT_UNTIL_FREE should never be passed as an argument to LWLockAcquire functions
- The mode determines lock compatibility and affects lock queue management
- Essential for PostgreSQL's concurrency control and shared memory protection