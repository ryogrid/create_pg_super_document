# proclock_hash

## Location
[src/backend/storage/lmgr/lock.c:521-551](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L521-L551)

## Overview
A specialized hash function that computes the hash code for a PROCLOCKTAG, ensuring that PROCLOCKs fall into the same partition as their associated LOCKs in the shared hash tables.

## Definition
static uint32 proclock_hash(const void *key, Size keysize)

## Detailed Description
The proclock_hash function is crucial for maintaining partition alignment between LOCK and PROCLOCK hash tables. PostgreSQL uses a single set of partition locks for both hash tables, so PROCLOCKs must map to the same partition number as their associated LOCKs. This is achieved by ensuring that a PROCLOCKTAG's hash code has the same low-order bits as its associated LOCKTAG's hash code, since dynahash.c uses the low-order bits to determine the partition number.

The function works by first computing the hash code of the associated LOCK object, then XORing the PGPROC address (left-shifted to preserve partition bits) to make the hash also depend on the specific process holding the lock.

## Parameters / Member Variables
- key: Pointer to the PROCLOCKTAG structure to be hashed
- keysize: Size of the key structure (expected to be sizeof(PROCLOCKTAG))

## Dependencies
- Functions called/Symbols referenced:
  - [LockTagHashCode](../L/LockTagHashCode.md): Computes hash code for the associated LOCK's tag
  - [PointerGetDatum](../P/PointerGetDatum.md): Converts pointer to Datum for hash computation
  - LOG2_NUM_LOCK_PARTITIONS: Used to left-shift the process pointer to preserve partition bits
- Called from (representative examples):
  - [InitLocks](../I/InitLocks.md): Used during lock manager initialization
  - PROCLOCK_PRINT: Used for debugging/printing PROCLOCK information

## Notes and Other Information
- This is a static function within lock.c, not exposed externally
- The hash function deliberately preserves the low-order partition bits from the LOCK hash
- The PGPROC address is left-shifted by LOG2_NUM_LOCK_PARTITIONS to avoid affecting partition assignment
- Uses intermediate Datum variable to suppress compiler warnings about pointer-to-int casts
- Critical for the proper functioning of PostgreSQL's lock partitioning scheme