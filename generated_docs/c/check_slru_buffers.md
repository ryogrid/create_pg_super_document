# check_slru_buffers

## Location
[src/backend/access/transam/slru.c:355-374](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L355-L374)

## Overview
check_slru_buffers is a GUC (Grand Unified Configuration) validation helper function that ensures SLRU buffer count settings are multiples of SLRU_BANK_SIZE.

## Definition


## Detailed Description
This function serves as a validation hook for PostgreSQL's configuration system (GUC) to validate SLRU buffer count parameters. It enforces the architectural requirement that all SLRU buffer counts must be multiples of SLRU_BANK_SIZE.

The banking constraint is essential because:
1. **Lock partitioning**: SLRU uses a banking system where buffers are organized into banks of SLRU_BANK_SIZE buffers each
2. **Concurrency optimization**: Each bank has its own lock, reducing contention compared to a single global lock
3. **Load balancing**: Banking ensures even distribution of buffers across lock partitions

When validation fails, the function uses GUC_check_errdetail() to provide a user-friendly error message explaining that the parameter must be a multiple of SLRU_BANK_SIZE.

The function follows the GUC check_hook interface:
- Returns true if the value is valid
- Returns false and sets an error message if invalid
- Takes the parameter name for error reporting
- Receives a pointer to the new value being validated

## Parameters / Member Variables
- : The name of the GUC parameter being validated (used in error messages)
- : Pointer to the new value being set for the SLRU buffer count parameter

## Dependencies
- Functions called/Symbols referenced:
  - SLRU_BANK_SIZE (constant defining buffers per bank)
  - GUC_check_errdetail (GUC system function for setting detailed error messages)

- Called from (representative examples):
  - [check_transaction_buffers](check_transaction_buffers.md) (for transaction status buffer validation)
  - check_commit_ts_buffers (for commit timestamp buffer validation)
  - [check_multixact_offset_buffers](check_multixact_offset_buffers.md) (for MultiXact offset buffer validation)
  - [check_multixact_member_buffers](check_multixact_member_buffers.md) (for MultiXact member buffer validation)
  - check_subtrans_buffers (for subtransaction buffer validation)
  - [check_notify_buffers](check_notify_buffers.md) (for async notification buffer validation)
  - check_serial_buffers (for serializable isolation buffer validation)

## Notes and Other Information
- This function is part of PostgreSQL's configuration validation system and is called whenever SLRU buffer parameters are modified
- The banking requirement is architectural and cannot be bypassed - all SLRU implementations must respect this constraint
- The error message format helps users understand the specific multiple required (typically 16 for SLRU_BANK_SIZE)
- This validation occurs at configuration time, preventing runtime errors that would occur if invalid buffer counts were used
- Different SLRU subsystems (CLOG, SUBTRANS, MultiXact, etc.) all use this same validation function through their specific check_hook implementations
- The validation helps maintain the performance benefits of the banking system across all SLRU instances