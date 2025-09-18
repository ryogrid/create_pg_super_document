# xl_testcustomrmgrs_message

## Location
src/test/modules/test_custom_rmgrs/test_custom_rmgrs.c: 32 - 36

## Overview
A struct that defines the format of WAL records for the test custom resource manager, containing a message size field and a flexible array member for the actual message payload.

## Definition


## Detailed Description
The `xl_testcustomrmgrs_message` structure serves as the WAL record format for PostgreSQL's test custom resource manager module. This structure is designed to store simple textual messages in Write-Ahead Log (WAL) records for testing purposes. It follows PostgreSQL's standard pattern for WAL record structures by using a flexible array member to accommodate variable-length payloads while maintaining efficient memory layout.

The structure is part of PostgreSQL's testing infrastructure for custom WAL resource managers, allowing developers to understand how custom resource managers work and how to implement WAL logging for extension modules. The resource manager provides no-op redo functionality and no decoding, making it a minimal example for educational purposes.

## Parameters / Member Variables
- `message_size`: A `Size` type field that stores the length of the message payload in bytes, used to determine how much data to read from the flexible array member
- `message`: A flexible array member of type `char` that contains the actual message payload data

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (PostgreSQL macro for flexible array members)
- Called from (representative examples):
  - SizeOfTestCustomRmgrsMessage (macro that calculates the fixed size of the structure)
  - testcustomrmgrs_desc (function that describes WAL record contents for debugging)
  - test_custom_rmgrs_insert_wal_record (function that creates WAL records using this structure)

## Notes and Other Information
- The structure is defined in `src/test/modules/test_custom_rmgrs/test_custom_rmgrs.c:32-36`
- A companion macro `SizeOfTestCustomRmgrsMessage` calculates the size of the fixed portion of the structure using `offsetof(xl_testcustomrmgrs_message, message)`
- The structure is used with WAL record type `XLOG_TEST_CUSTOM_RMGRS_MESSAGE` (0x00)
- This is part of the experimental resource manager with ID `RM_EXPERIMENTAL_ID`
- The structure follows PostgreSQL's naming convention for WAL record structures (prefixed with `xl_`)
- When creating WAL records, the fixed portion and variable portion are registered separately using `XLogRegisterData`
- The test module marks records as unimportant using `XLOG_MARK_UNIMPORTANT` flag