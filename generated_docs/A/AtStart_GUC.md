# AtStart_GUC

## Location
src/backend/utils/misc/guc.c: 2217 - 2236

## Overview
AtStart_GUC initializes the GUC (Grand Unified Configuration) nesting level at the beginning of a main transaction and validates the system state.

## Definition


## Detailed Description
This function performs essential initialization for the GUC system at the start of each main transaction. Its primary responsibility is to set the GUC nesting level to 1, indicating that the system is now inside a transaction context. The function also includes important validation logic to detect potential issues with the GUC state management system.

Key behaviors:
- Validates that GUCNestLevel is 0 between transactions (proper cleanup from previous transactions)
- Issues a WARNING if the nest level is incorrect, indicating a potential bug in transaction cleanup
- Sets GUCNestLevel to 1 to indicate entry into transaction context
- Does not attempt to fix corrupted state beyond issuing a warning

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - elog (for warning messages)
  - GUCNestLevel (global variable)
- Called from (representative examples):
  - [StartTransaction](../S/StartTransaction.md) (transaction management)
  - Functions that initialize transaction state

## Notes and Other Information
- This is a public function declared in guc.h and called from transaction management code
- The function serves as a critical checkpoint for detecting GUC system integrity issues
- GUCNestLevel = 1 represents the base transaction level (not subtransaction level)
- The warning message helps identify bugs in transaction cleanup or improper nesting
- Part of PostgreSQL's transactional configuration system that ensures GUC changes can be properly rolled back
- Should always be paired with a corresponding AtEOXact_GUC call at transaction end