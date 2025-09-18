# test_slru_page_writeall

## Location
src/test/modules/test_slru/test_slru.c: 85 - 91

## Overview
A PostgreSQL function that forces all dirty pages in the test SLRU cache to be written to disk, providing a mechanism for bulk page synchronization during testing.

## Definition


## Detailed Description
This function provides a simple testing interface for performing bulk write operations on an SLRU cache. It calls  with the flush parameter set to true, which causes all dirty pages in the SLRU cache to be written to disk and flushed to ensure durability. This is particularly useful in testing scenarios where you need to ensure all pending writes have been persisted before proceeding with subsequent operations or verification steps.

## Parameters / Member Variables
- No function parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface)

## Dependencies
- Functions called/Symbols referenced:
  - SimpleLruWriteAll (performs the actual bulk write operation)
  - TestSlruCtl (global SLRU control structure for the test module)
  - PG_RETURN_VOID (PostgreSQL function return macro)
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- This is a PostgreSQL C function that can be called from SQL
- The function is very simple, serving as a wrapper around SimpleLruWriteAll
- The  parameter to SimpleLruWriteAll indicates that a flush operation should be performed
- No locking is required as SimpleLruWriteAll handles its own synchronization
- Useful for testing scenarios that require ensuring all data is persisted to disk
- Part of the SLRU testing infrastructure for validating bulk write operations