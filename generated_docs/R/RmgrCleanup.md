# RmgrCleanup

## Location
src/backend/access/transam/rmgr.c: 74 - 90

## Overview
Performs cleanup operations for all registered resource managers by calling their cleanup routines during shutdown or end of WAL recovery.

## Definition
void RmgrCleanup(void)

## Detailed Description
RmgrCleanup iterates through all possible resource manager IDs (from 0 to RM_MAX_ID) and calls the cleanup routine (rm_cleanup) for each registered resource manager that has one defined. This function is typically called at the end of WAL recovery or during system shutdown to allow resource managers to perform any necessary cleanup operations.

The function checks if each resource manager ID exists using RmgrIdExists() and only calls the cleanup routine if it's not NULL, ensuring safe operation even when some resource managers don't require cleanup procedures.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - RmgrIdExists
  - RM_MAX_ID
  - RmgrTable[rmid].rm_cleanup
- Called from (representative examples):
  - [PerformWalRecovery](../P/PerformWalRecovery.md)

## Notes and Other Information
- Located in src/backend/access/transam/rmgr.c:74-90
- This is part of the resource manager infrastructure that allows extensions to register custom WAL resource managers
- The cleanup routines are called in resource manager ID order
- Resource managers can use their cleanup routine to free resources, close files, or perform other teardown tasks after WAL processing is complete