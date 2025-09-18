# SimpleLruGetBankLock

## Location
src/include/access/slru.h: 175 - 198

## Overview
SimpleLruGetBankLock is a static inline function that retrieves the appropriate bank lock for a given SLRU (Simple LRU) page number in PostgreSQL's buffer management system.

## Definition


## Detailed Description
This function implements a critical part of PostgreSQL's SLRU (Simple Least Recently Used) buffer management by providing thread-safe access to SLRU pages through bank-based locking. The SLRU system divides its buffer pool into multiple banks, each protected by its own lightweight lock (LWLock). This banking approach reduces lock contention by allowing concurrent access to pages in different banks.

The function calculates which bank a given page belongs to using a modulo operation on the page number, then returns the corresponding lock from the shared data structure. This lock must be acquired before accessing any buffer slots within that bank, ensuring data consistency in a multi-process environment.

## Parameters / Member Variables
- : SlruCtl pointer - Control structure for the SLRU cache containing shared data and bank configuration
- : int64 - The logical page number for which to retrieve the bank lock

## Dependencies
- Functions called/Symbols referenced:
  - SlruCtl (control structure type)
  - LWLock (lightweight lock type)
  - SlruCtlData.shared (shared data structure)
  - SlruCtlData.nbanks (number of banks)
  - SlruSharedData.bank_locks (array of bank locks)

- Called from (representative examples):
  - TransactionIdSetPageStatus (clog.c:305)
  - TransactionIdGetStatus (clog.c:755)
  - RecordNewMultiXact (multixact.c:925)
  - SimpleLruZeroPage (slru.c:380)
  - SimpleLruReadPage (slru.c:506)
  - SubTransSetParent (subtrans.c:96)
  - SerialAdd (predicate.c:870)

## Notes and Other Information
- This is a static inline function defined in slru.h header for performance optimization
- The banking system helps reduce lock contention in high-concurrency scenarios
- Used extensively throughout PostgreSQL's transaction management subsystems including CLOG, commit timestamps, multixact, and subtransactions
- The returned lock must be held in appropriate mode (shared or exclusive) depending on the operation being performed on the SLRU page
- Bank number calculation uses simple modulo arithmetic: 