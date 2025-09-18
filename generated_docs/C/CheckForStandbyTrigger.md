# CheckForStandbyTrigger

## Location
src/backend/access/transam/xlogrecovery.c: 4434 - 4454

## Overview
Checks whether a promotion request has been issued for the standby server, either locally or through signal files.

## Definition


## Detailed Description
This function serves as the central checkpoint for detecting standby promotion requests in PostgreSQL's WAL recovery process. It implements a two-tier checking mechanism: first, it examines the local promotion flag () for immediate detection of already-processed promotion requests. If no local trigger is found, it checks for external promotion signals through  and validates them with . When a valid promotion signal is detected, the function logs the promotion request, cleans up signal files, resets the signal state, and triggers the promotion process by calling . This function is called at various critical points during WAL recovery to ensure timely response to promotion requests.

## Parameters / Member Variables
This function takes no parameters and returns a boolean value indicating whether promotion has been triggered.

## Dependencies
- Functions called/Symbols referenced:
  - IsPromoteSignaled
  - CheckPromoteSignal
  - ereport
  - RemovePromoteSignalFiles
  - ResetPromoteSignaled
  - SetPromoteIsTriggered
  - LocalPromoteIsTriggered (global variable)
- Called from (representative examples):
  - recoveryPausesHere
  - recoveryApplyDelay
  - ReadRecord
  - WaitForWALToBecomeAvailable
  - RecoveryRequiresIntParameter

## Notes and Other Information
- This function is static and only accessible within the xlogrecovery.c file
- Returns true immediately if LocalPromoteIsTriggered is already set for efficiency
- Logs promotion requests at LOG level when detected
- Performs complete signal cleanup and state transitions when promotion is triggered
- Called frequently during recovery to ensure responsive promotion handling
- Located at src/backend/access/transam/xlogrecovery.c:4434-4454