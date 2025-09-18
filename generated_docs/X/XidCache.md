# XidCache

## Location
[src/include/storage/proc.h:49-56](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/proc.h#L49-L56)

## Overview
XidCache is a struct that caches subtransaction XIDs for a PostgreSQL backend process, storing up to 64 subtransaction IDs to optimize transaction visibility checks.

## Definition


## Detailed Description
The XidCache structure is designed to cache subtransaction XIDs (transaction identifiers) for optimization purposes in PostgreSQL's transaction management system. Each backend process maintains a cache of up to PGPROC_MAX_CACHED_SUBXIDS (64) TransactionIds for non-aborted subtransactions of its current top transaction. This cache helps other backends quickly determine which transactions are currently running without having to consult the more expensive pg_subtrans system.

The cache is part of PostgreSQL's visibility checking mechanism. When determining if a transaction is visible, other backends can first check these cached XIDs. If the cache hasn't overflowed and an XID isn't found in any PGPROC array, it can be assumed the transaction is not running. However, if any cache has overflowed, backends must fall back to checking pg_subtrans for a definitive answer.

## Parameters / Member Variables
- : Array storing up to 64 TransactionId values representing non-aborted subtransactions of the current top transaction

## Dependencies
- Functions called/Symbols referenced:
  - PGPROC_MAX_CACHED_SUBXIDS (constant defining cache size as 64)
- Called from (representative examples):
  - [PGPROC](../P/PGPROC.md) (used as a member of the PGPROC structure)

## Notes and Other Information
- The cache size of 64 subtransactions is noted as a "guessed-at value" and may be subject to tuning
- Cache overflow handling is critical - when overflow occurs, the system must fall back to pg_subtrans lookups
- This mechanism is part of PostgreSQL's transaction visibility optimization strategy
- Related test specifications can be found in src/test/isolation/specs/subxid-overflow.spec