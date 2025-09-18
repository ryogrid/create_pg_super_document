# WalSummarizerShmemSize

## Location
src/backend/postmaster/walsummarizer.c: 171 - 179

## Overview
Returns the amount of shared memory required for the WAL summarizer module.

## Definition


## Detailed Description
This function calculates and returns the shared memory space needed for WAL summarizer functionality. It simply returns the size of the WalSummarizerData structure, which contains all the shared state information needed for coordinating WAL summarization activities across PostgreSQL processes.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - WalSummarizerData (returns sizeof this structure)
- Called from (representative examples):
  - CalculateShmemSize (in src/backend/storage/ipc/ipci.c:145)
  - WalSummarizerShmemInit (in src/backend/postmaster/walsummarizer.c:185)

## Notes and Other Information
- This is a utility function used during PostgreSQL startup to calculate total shared memory requirements
- The function is declared in src/include/postmaster/walsummarizer.h
- WalSummarizerData contains fields for tracking summarization progress, timeline info, LSN positions, and process coordination
- Location: src/backend/postmaster/walsummarizer.c:171-179