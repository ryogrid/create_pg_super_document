# AtStart_Cache

## Location
src/backend/access/transam/xact.c: 1164 - 1172

## Overview
AtStart_Cache is a static function that handles cache invalidation processing at the start of a new transaction by accepting and processing any pending invalidation messages.

## Definition
```c
static void AtStart_Cache(void)
```

## Detailed Description
AtStart_Cache is part of the transaction startup sequence in PostgreSQL. This function is responsible for ensuring that the current backend processes any pending cache invalidation messages that may have accumulated before starting a new transaction. By calling AcceptInvalidationMessages(), it ensures that the backend's local caches (such as system catalog caches) are synchronized with the current state of the database before beginning transaction work.

This function is critical for maintaining cache coherency across different backends and ensuring that each transaction starts with a consistent view of cached system information.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [AcceptInvalidationMessages](AcceptInvalidationMessages.md) (at line 1166)
- Called from (representative examples):
  - [StartTransaction](../S/StartTransaction.md) (src/backend/access/transam/xact.c:2155)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the xact.c file
- Part of the StartTransaction infrastructure that ensures proper initialization of various subsystems
- Cache invalidation is crucial for maintaining data consistency in a multi-backend environment
- The function is called early in the transaction startup sequence to ensure clean cache state