# test_resowner_forget_between_phases

## Location
src/test/modules/test_resowner/test_resowner_basic.c: 188 - 211

## Overview
A PostgreSQL test function that validates that ResourceOwnerForget properly fails when called after a ResourceOwnerRelease operation, testing the resource owner lifecycle state management.

## Definition


## Detailed Description
This function is a negative test case designed to verify that the ResourceOwner subsystem correctly enforces lifecycle rules. It creates a resource owner, remembers a string resource, releases the resource owner in the BEFORE_LOCKS phase, and then attempts to forget the previously remembered resource. The function expects this sequence to fail with an error because ResourceOwnerForget should not be allowed after ResourceOwnerRelease has been called.

The test follows this sequence:
1. Creates a new ResourceOwner as a child of CurrentResourceOwner
2. Enlarges the resource owner capacity and remembers a string resource
3. Releases the resource owner in the BEFORE_LOCKS phase
4. Attempts to forget the string resource (this should fail)
5. If the forget operation doesn't fail as expected, the function logs an error

The function is designed to never return normally - it expects ResourceOwnerForget to throw an error, making the final elog(ERROR) line unreachable.

## Parameters / Member Variables
This function takes no meaningful parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface).

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerCreate
  - ResourceOwnerEnlarge  
  - CStringGetDatum
  - ResourceOwnerRemember
  - ResourceOwnerRelease
  - ResourceOwnerForget
  - PG_RETURN_VOID
  - RESOURCE_RELEASE_BEFORE_LOCKS (constant)
  - string_desc (static resource descriptor)

- Called from (representative examples):
  - test_resowner_remember_between_phases

## Notes and Other Information
- This is part of the test_resowner PostgreSQL extension's test suite
- Located in src/test/modules/test_resowner/test_resowner_basic.c:188-211
- The function uses the static string_desc resource descriptor which defines a test resource type with RESOURCE_RELEASE_AFTER_LOCKS phase and RELEASE_PRIO_FIRST priority
- The test string resource "my string" is used as test data
- This is a negative test case - success is measured by the function throwing an expected error
- The function demonstrates proper resource owner lifecycle management and phase-based resource cleanup validation
- Related to test_resowner_remember_between_phases which tests a similar scenario for the remember operation