# PredXactList

## Location
src/include/storage/predicate_internals.h: 177 - 178

## Overview
PredXactList is a typedef that defines a pointer to PredXactListData, serving as the handle for accessing the shared memory control structure that manages serializable transactions.

## Definition
```c
typedef struct PredXactListData *PredXactList;
```

## Detailed Description
PredXactList is a simple pointer typedef that provides a convenient handle for accessing the PredXactListData structure in shared memory. This typedef is used throughout the predicate locking system to reference the global control structure that manages serializable transaction state. It abstracts the pointer nature of the access, making the code more readable and type-safe.

## Parameters / Member Variables
This is a typedef for a pointer, so it has no direct member variables. It points to a PredXactListData structure which contains:
- Transaction lists (available and active)
- Global transaction state variables
- Cleanup coordination fields
- Memory management structures

## Dependencies
- Functions called/Symbols referenced:
  - PredXactListData
- Called from (representative examples):
  - SerialControl (within shared memory management)

## Notes and Other Information
- Simple typedef for improved code readability and type safety
- Used as the primary interface for accessing global serializable transaction state
- Part of PostgreSQL's shared memory management for SSI implementation
- Enables clean abstraction of pointer-based access to shared control structures
- Critical component for coordinating serializable transaction management across multiple backend processes