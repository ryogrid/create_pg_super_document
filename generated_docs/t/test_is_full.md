# test_is_full

## Location
src/test/modules/test_tidstore/test_tidstore.c: 314 - 326

## Overview
A simple test function that verifies TidStore memory usage functionality by checking if the current memory usage exceeds the baseline empty store size.

## Definition


## Detailed Description
This function provides a basic test for the TidStoreMemoryUsage functionality by comparing the current memory usage of the TidStore against a baseline measurement taken when the store was empty. Rather than implementing complex memory limit checking (which would be used in production), this function focuses on verifying that the memory usage reporting mechanism is functioning correctly.

The function serves as a validation tool to ensure that TidStoreMemoryUsage returns meaningful values that change as the store accumulates data, which is essential for memory management in real-world PostgreSQL deployments.

## Parameters / Member Variables
- No explicit parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface)
- Uses global  for TidStore operations
- References global  for baseline memory comparison

## Dependencies
- Functions called/Symbols referenced:
  - [check_tidstore_available](../c/check_tidstore_available.md) - Validates that tidstore is available for operations
  - [TidStoreMemoryUsage](../T/TidStoreMemoryUsage.md) - Returns current memory usage of the TidStore
- Called from (representative examples):
  - No direct references found (likely called via SQL interface in tests)

## Notes and Other Information
- Located in src/test/modules/test_tidstore/test_tidstore.c:314-326
- Designed specifically for testing memory usage reporting rather than implementing production memory limits
- Uses a simple threshold comparison against the empty store baseline to determine 'fullness'
- Returns a boolean value indicating whether the store has grown beyond its initial empty state
- Part of the PostgreSQL testing infrastructure to validate TidStore memory management capabilities
- The 'fullness' concept here is purely for testing validation, not for actual capacity management
- Essential for verifying that memory usage tracking works correctly across different TidStore operations