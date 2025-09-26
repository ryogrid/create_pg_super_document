# TwoPhaseShmemSize

## Location
[src/backend/access/transam/twophase.c:237-252](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L237-L252)

## Overview
Calculates the amount of shared memory space needed for the two-phase commit subsystem during PostgreSQL initialization.

## Definition
Size TwoPhaseShmemSize(void)

## Detailed Description
This function computes the total shared memory size required for managing prepared transactions in PostgreSQL's two-phase commit protocol. It calculates space for the main TwoPhaseStateData structure, an array of GlobalTransaction pointers, and the actual GlobalTransactionData structures. The calculation is based on the max_prepared_xacts configuration parameter, which determines the maximum number of prepared transactions that can exist simultaneously.

The function performs careful size calculations using PostgreSQL's safe arithmetic functions (add_size, mul_size) to prevent integer overflow, and applies MAXALIGN to ensure proper memory alignment for the allocated structures.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [add_size](../a/add_size.md) (safe addition to prevent overflow)
  - [mul_size](../m/mul_size.md) (safe multiplication to prevent overflow)
  - MAXALIGN (alignment macro)
  - offsetof (standard C macro)
- Types referenced:
  - [TwoPhaseStateData](TwoPhaseStateData.md) (main state structure)
  - [GlobalTransaction](../G/GlobalTransaction.md) (pointer type)
  - [GlobalTransactionData](../G/GlobalTransactionData.md) (actual transaction data structure)
- Called from:
  - [TwoPhaseShmemInit](TwoPhaseShmemInit.md) (in twophase.c:258)
  - [CalculateShmemSize](../C/CalculateShmemSize.md) (in ipci.c:130)

## Notes and Other Information
- The calculation depends on the max_prepared_xacts GUC parameter
- Uses safe arithmetic functions to prevent integer overflow during size calculations
- Memory alignment is enforced through MAXALIGN to ensure proper structure alignment
- This function is called during PostgreSQL startup to determine shared memory requirements
- Part of the shared memory initialization sequence for two-phase commit support