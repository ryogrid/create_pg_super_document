# SerialControl

## Location
src/backend/storage/lmgr/predicate.c: 352 - 497

## Overview
SerialControl is a pointer type to SerialControlData that serves as the main handle for accessing serializable transaction control data in PostgreSQL's predicate locking system.

## Definition


## Detailed Description
SerialControl is a typedef that creates a pointer type to SerialControlData structures. It serves as the primary interface for accessing and manipulating the control data for PostgreSQL's Serializable Snapshot Isolation (SSI) implementation. The static global variable serialControl holds the main instance used throughout the predicate locking subsystem.

This type is part of the broader predicate locking infrastructure that prevents serialization anomalies by tracking read-write conflicts between concurrent serializable transactions. The SerialControl pointer provides access to SLRU metadata including page boundaries and transaction ID ranges.

## Parameters / Member Variables
- This is a pointer type to SerialControlData, so it provides access to:
  - : Newest initialized SLRU page
  - : Newest valid transaction ID in the SLRU 
  - : Oldest transaction ID of potential interest

## Dependencies
- Functions called/Symbols referenced:
  - [SerialControlData](SerialControlData.md) (base structure type)
  - [SERIALIZABLEXACT](SERIALIZABLEXACT.md) (related transaction structure)
  - [PredXactList](../P/PredXactList.md) (predicate transaction list)
  - [RWConflictPoolHeader](../R/RWConflictPoolHeader.md) (read-write conflict pool)
  - [HTAB](../H/HTAB.md) (hash table type for various predicate lock tables)
  - [PREDICATELOCKTARGETTAG](../P/PREDICATELOCKTARGETTAG.md) (predicate lock target identification)
- Called from (representative examples):
  - SerialInit (initialization function)
  - Various predicate locking functions that access serialControl

## Notes and Other Information
- The static serialControl variable is the main global instance used throughout the predicate locking system
- This type provides the primary interface for managing serializable transaction metadata in shared memory
- Part of the SLRU-based system for efficiently tracking serializable transaction conflicts
- Used in conjunction with multiple hash tables (SerializableXidHash, PredicateLockTargetHash, PredicateLockHash) for comprehensive conflict detection
- Critical component of PostgreSQL's SERIALIZABLE isolation level implementation