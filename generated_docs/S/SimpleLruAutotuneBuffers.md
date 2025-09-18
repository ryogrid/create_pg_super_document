# SimpleLruAutotuneBuffers

## Location
[src/backend/access/transam/slru.c:232-251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L232-L251)

## Overview
SimpleLruAutotuneBuffers automatically determines the optimal number of SLRU buffers based on the total shared buffer pool size, using a divisor-based calculation with configurable limits.

## Definition


## Detailed Description
This function provides an automatic tuning mechanism for SLRU buffer allocation by calculating an appropriate number of buffers relative to the main shared buffer pool (NBuffers). The algorithm follows these steps:

1. **Calculate base allocation**: Divide the total shared buffers (NBuffers) by the given divisor
2. **Apply minimum constraint**: Ensure at least SLRU_BANK_SIZE buffers are allocated
3. **Apply maximum constraint**: Cap the result at the specified maximum value
4. **Bank alignment**: Round both the calculated value and maximum down to the nearest multiple of SLRU_BANK_SIZE

The banking alignment is crucial because SLRU uses a banking system to reduce lock contention, where buffers are organized into banks of SLRU_BANK_SIZE buffers each.

The function uses nested Min/Max operations to ensure all constraints are satisfied:
- Min() applies the maximum cap (after bank alignment)
- Max() ensures the minimum of SLRU_BANK_SIZE is met
- Modulo operations (%) provide bank alignment by rounding down

## Parameters / Member Variables
- : The divisor to apply to NBuffers (shared_buffers). Larger values result in fewer SLRU buffers
- : Maximum number of SLRU buffers allowed, regardless of the calculated value

## Dependencies
- Functions called/Symbols referenced:
  - SLRU_BANK_SIZE (constant defining buffers per bank)
  - NBuffers (global variable representing total shared buffers)
  - Min (macro for minimum of two values)
  - Max (macro for maximum of two values)

- Called from (representative examples):
  - CLOGShmemBuffers
  - CommitTsShmemBuffers  
  - [SUBTRANSShmemBuffers](SUBTRANSShmemBuffers.md)
  - [SimpleLruGetBankLock](SimpleLruGetBankLock.md)

## Notes and Other Information
- This function enables automatic scaling of SLRU buffers based on available shared memory
- Different SLRU subsystems can use different divisor values to allocate proportionally appropriate buffer counts
- The banking constraint ensures efficient lock management in the SLRU implementation
- The algorithm guarantees that the result is always a multiple of SLRU_BANK_SIZE
- Typical usage involves calling this during system initialization to determine buffer counts for various SLRU instances (CLOG, SUBTRANS, CommitTS, etc.)
- The divisor parameter allows fine-tuning the proportion of shared buffers allocated to each SLRU subsystem