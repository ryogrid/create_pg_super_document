# TwoPhaseShmemSize

## Location
src/backend/access/transam/twophase.c: 237 - 252

## Overview
Calculates the amount of shared memory space needed for the two-phase commit subsystem during PostgreSQL initialization.

## Definition
Size TwoPhaseShmemSize(void)

## Detailed Description
This function computes the total shared memory size required for managing prepared transactions in PostgreSQL's two-phase commit protocol. It calculates space for the main TwoPhaseStateData structure, an array of GlobalTransaction pointers, and the actual GlobalTransactionData structures. The calculation is based on the max_prepared_xacts configuration parameter, which determines the maximum number of prepared transactions that can exist simultaneously.

The function performs careful size calculations using PostgreSQL's safe arithmetic functions (add_size, mul_size) to prevent integer overflow, and applies MAXALIGN to ensure proper memory alignment for the allocated structures.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - add_size (safe addition to prevent overflow)
  - mul_size (safe multiplication to prevent overflow)
  - MAXALIGN (alignment macro)
  - offsetof (standard C macro)
- Types referenced:
  - TwoPhaseStateData (main state structure)
  - GlobalTransaction (pointer type)
  - GlobalTransactionData (actual transaction data structure)
- Called from:
  - TwoPhaseShmemInit (in twophase.c:258)
  - CalculateShmemSize (in ipci.c:130)

## Notes and Other Information
- The calculation depends on the max_prepared_xacts GUC parameter
- Uses safe arithmetic functions to prevent integer overflow during size calculations
- Memory alignment is enforced through MAXALIGN to ensure proper structure alignment
- This function is called during PostgreSQL startup to determine shared memory requirements
- Part of the shared memory initialization sequence for two-phase commit support