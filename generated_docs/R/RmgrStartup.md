# RmgrStartup

## Location
src/backend/access/transam/rmgr.c: 58 - 73

## Overview
Initializes all registered resource managers by calling their startup routines during WAL recovery or startup.

## Definition


## Detailed Description
RmgrStartup iterates through all possible resource manager IDs (from 0 to RM_MAX_ID) and calls the startup routine (rm_startup) for each registered resource manager that has one defined. This function is typically called during PostgreSQL startup or WAL recovery to allow resource managers to perform any necessary initialization before WAL replay begins.

The function checks if each resource manager ID exists using RmgrIdExists() and only calls the startup routine if it's not NULL, ensuring safe operation even when some resource managers don't require startup procedures.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - RmgrIdExists
  - RM_MAX_ID
  - RmgrTable[rmid].rm_startup
- Called from (representative examples):
  - PerformWalRecovery

## Notes and Other Information
- Located in src/backend/access/transam/rmgr.c:58-73
- This is part of the resource manager infrastructure that allows extensions to register custom WAL resource managers
- The startup routines are called in resource manager ID order
- Resource managers can use their startup routine to initialize data structures or perform other setup tasks needed before WAL processing begins