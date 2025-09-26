# check_subtrans_buffers

## Location
[src/backend/access/transam/subtrans.c:254-269](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/subtrans.c#L254-L269)

## Overview
check_subtrans_buffers is a GUC (Grand Unified Configuration) check hook function that validates the subtransaction_buffers configuration parameter.

## Definition
```c
bool check_subtrans_buffers(int *newval, void **extra, GucSource source)
```

## Detailed Description
This function serves as a validation hook for the subtransaction_buffers configuration parameter in PostgreSQL's GUC system. It ensures that any new value assigned to subtransaction_buffers meets the required constraints by delegating the actual validation to the generic check_slru_buffers function. The validation ensures that the buffer count is a multiple of SLRU_BANK_SIZE, which is a requirement for the Simple LRU buffer management system used by SUBTRANS.

## Parameters / Member Variables
- `newval`: Pointer to the new integer value being assigned to subtransaction_buffers
- `extra`: Pointer to extra data (unused in this function, passed through to check_slru_buffers)
- `source`: The source of the configuration change (e.g., config file, SQL command, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [check_slru_buffers](check_slru_buffers.md)
- Types referenced:
  - GucSource
- Called from (representative examples):
  - GUC system (via function pointer registration)

## Notes and Other Information
- This is a GUC check hook function, called automatically by the PostgreSQL configuration system
- Returns true if the new value is valid, false otherwise
- The actual validation logic is in check_slru_buffers, which ensures the value is a multiple of SLRU_BANK_SIZE
- Part of PostgreSQL's configuration validation framework for SLRU-based subsystems
- Located in src/backend/access/transam/subtrans.c:254-269