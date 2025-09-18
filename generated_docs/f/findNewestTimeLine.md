# findNewestTimeLine

## Location
src/backend/access/transam/timeline.c: 264 - 303

## Overview
Finds the newest existing timeline starting from a given timeline ID by probing for timeline history files.

## Definition
```c
TimeLineID findNewestTimeLine(TimeLineID startTLI)
```

## Detailed Description
This function implements a simple algorithm to discover the highest timeline ID that currently exists, starting from a given baseline timeline. It works by incrementally probing for the existence of timeline history files, assuming that timelines are created sequentially without gaps.

The function starts with the provided startTLI and incrementally checks each subsequent timeline ID (startTLI + 1, startTLI + 2, etc.) by calling existsTimeLineHistory(). It continues until it finds a timeline ID for which no history file exists, indicating that this is likely the first unused timeline ID. The function then returns the highest timeline ID for which a history file was found.

An important guarantee provided by this function is that (result + 1) represents a timeline ID that is not currently in use, making it safe to assign to a new timeline during operations like standby promotion.

## Parameters / Member Variables
- `startTLI`: The starting timeline ID from which to begin the search (this timeline is assumed to exist)

## Dependencies
- Functions called/Symbols referenced:
  - existsTimeLineHistory - checks for existence of timeline history files
- Called from (representative examples):
  - StartupXLOG - during database startup to determine current timeline state
  - validateRecoveryParameters - during recovery parameter validation
  - rescanLatestTimeLine - when rescanning timeline information

## Notes and Other Information
- The algorithm assumes sequential timeline IDs without gaps, though comments suggest this assumption might be reconsidered
- The function provides a strong guarantee that the returned value + 1 is safe for new timeline assignment
- Used primarily during recovery operations and standby promotion scenarios
- The search is performed by probing timeline history files rather than examining WAL files directly
- Located in src/backend/access/transam/timeline.c:264-303