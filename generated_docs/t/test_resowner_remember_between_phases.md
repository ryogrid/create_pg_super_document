# test_resowner_remember_between_phases

## Location
src/test/modules/test_resowner/test_resowner_basic.c: 165 - 187

## Overview
test_resowner_remember_between_phases is a PostgreSQL SQL-callable function that tests the resource owner's validation of resource registration attempts during the resource release process.

## Definition
```c
Datum test_resowner_remember_between_phases(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a negative test case for PostgreSQL's resource owner system, specifically testing the enforcement of proper resource management phase ordering. It creates a resource owner, initiates the resource release process by calling ResourceOwnerRelease for the BEFORE_LOCKS phase, and then attempts to register a new resource. This scenario should fail because the resource owner is already in the release phase and should not accept new resource registrations.

The function is designed to trigger an error condition when ResourceOwnerEnlarge is called after ResourceOwnerRelease has already been initiated, demonstrating that the resource owner correctly validates the state transitions and prevents improper resource registration during cleanup phases.

## Parameters / Member Variables
This function takes no parameters (uses PG_FUNCTION_ARGS macro but doesn't extract any arguments).

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerCreate (resource owner creation)
  - ResourceOwnerRelease (initiates resource release process)
  - ResourceOwnerEnlarge (attempts resource owner capacity expansion - should fail)
  - ResourceOwnerRemember (attempts resource registration - should not be reached)
  - CStringGetDatum (string to Datum conversion)
  - string_desc (global resource descriptor for string resources)
  - elog (error logging - for unreachable code path)
  - PG_RETURN_VOID (return macro)
- Called from (representative examples):
  - SQL queries invoking this test function

## Notes and Other Information
- Part of the test_resowner module for testing PostgreSQL resource management
- Designed as a negative test case that should trigger an error condition
- Tests the resource owner's state validation during phase transitions
- The elog(ERROR, "ResourceOwnerEnlarge should have errored out") line should never be reached if the system works correctly
- References string_desc which should be defined elsewhere in the module as a ResourceOwnerDesc
- Demonstrates PostgreSQL's defensive programming approach by validating operation sequences
- The function should terminate with an error before reaching the PG_RETURN_VOID() statement
- Useful for verifying that resource owners properly enforce phase ordering and prevent invalid state transitions