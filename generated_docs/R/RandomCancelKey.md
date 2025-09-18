# RandomCancelKey

## Location
src/backend/postmaster/postmaster.c: 3870 - 3879

## Overview
RandomCancelKey generates a cryptographically strong random 32-bit integer to be used as a cancel key for PostgreSQL backend processes.

## Definition
static bool RandomCancelKey(int32 *cancel_key)

## Detailed Description
RandomCancelKey is a security-critical function that generates cancel keys used in PostgreSQL client-server communication protocol. Cancel keys are part of PostgreSQL security mechanism that allows clients to request cancellation of running queries. Each backend process receives a unique, randomly generated cancel key during startup, which clients must provide when requesting query cancellation.

The function uses pg_strong_random() to ensure the generated keys are cryptographically secure and unpredictable. This is important because cancel keys serve as authentication tokens - without the correct cancel key, a client cannot cancel another session queries, preventing potential denial-of-service attacks.

## Parameters / Member Variables
- cancel_key: Pointer to int32 where the generated random cancel key will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strong_random](../p/pg_strong_random.md) (cryptographically strong random number generation)
- Called from (representative examples):
  - [BackendStartup](../B/BackendStartup.md) (during new backend process initialization)
  - [StartAutovacuumWorker](../S/StartAutovacuumWorker.md) (for autovacuum worker processes)
  - [assign_backendlist_entry](../a/assign_backendlist_entry.md) (when assigning backend entries)

## Notes and Other Information
- Returns boolean indicating success/failure of random number generation
- Critical for PostgreSQL security - prevents unauthorized query cancellation
- Uses cryptographically strong randomness rather than weaker pseudo-random generators
- Cancel keys are used in conjunction with backend process IDs for query cancellation protocol
- Part of PostgreSQL query cancellation and connection management security infrastructure