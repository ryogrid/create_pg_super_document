# TwoPhaseStateData

## Location
src/backend/access/transam/twophase.c: 176 - 186

## Overview
TwoPhaseStateData is the shared state structure that manages all two-phase commit transactions in PostgreSQL, providing centralized tracking and memory management for prepared transactions.

## Definition
```c
typedef struct TwoPhaseStateData
{
    /* Head of linked list of free GlobalTransactionData structs */
    GlobalTransaction freeGXacts;

    /* Number of valid prepXacts entries. */
    int         numPrepXacts;

    /* There are max_prepared_xacts items in this array */
    GlobalTransaction prepXacts[FLEXIBLE_ARRAY_MEMBER];
} TwoPhaseStateData;
```

## Detailed Description
This structure serves as the central control point for PostgreSQL's two-phase commit protocol implementation. It maintains shared state that is protected by TwoPhaseStateLock to ensure thread-safe access across multiple backend processes.

The structure manages two primary aspects:
1. **Memory Management**: Maintains a free list of GlobalTransaction structures to efficiently allocate and deallocate prepared transaction entries
2. **Active Transaction Tracking**: Keeps an array of currently prepared transactions with a count of valid entries

The prepXacts array is dynamically sized based on the max_prepared_xacts configuration parameter, allowing for flexible scaling based on system requirements.

## Parameters / Member Variables
- `freeGXacts`: Head pointer to a linked list of free GlobalTransactionData structures, enabling efficient memory reuse for prepared transactions
- `numPrepXacts`: Integer count of the number of valid/active entries currently in the prepXacts array
- `prepXacts`: Flexible array member containing pointers to GlobalTransaction structures representing currently prepared transactions (array size determined by max_prepared_xacts setting)

## Dependencies
- Functions called/Symbols referenced:
  - GlobalTransaction (typedef pointer to GlobalTransactionData)
  - FLEXIBLE_ARRAY_MEMBER (macro for variable-length array declaration)
- Called from (representative examples):
  - TwoPhaseShmemSize (for calculating required shared memory size)
  - TwoPhaseShmemInit (for initializing shared memory structures)

## Notes and Other Information
- Access to this structure must be synchronized using TwoPhaseStateLock to prevent race conditions
- The flexible array member design allows the structure to accommodate different max_prepared_xacts configurations without recompilation
- The free list mechanism (freeGXacts) provides efficient memory management by reusing GlobalTransactionData structures
- This is a shared memory structure that persists across backend process lifecycles
- The structure supports the complete lifecycle management of prepared transactions from creation through completion or abort