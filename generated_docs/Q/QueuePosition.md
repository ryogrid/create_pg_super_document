# QueuePosition

## Location
src/backend/commands/async.c: 194 - 198

## Overview
QueuePosition is a structure that describes a specific position within the asynchronous notification queue, used for tracking locations in the SLRU-based queue system.

## Definition


## Detailed Description
The QueuePosition structure serves as a coordinate system for locating specific positions within PostgreSQL's asynchronous notification queue. The queue is implemented using SLRU (Simple Least Recently Used) pages, and this structure provides a two-dimensional addressing scheme: a page number and an offset within that page. This allows precise positioning within the potentially very large notification queue that spans multiple SLRU pages.

## Parameters / Member Variables
- : SLRU page number identifying which page in the queue
- : Byte offset within the specified page, pinpointing the exact location

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)
- Called from (representative examples):
  - [QueueBackendStatus](QueueBackendStatus.md)
  - [AsyncQueueControl](../A/AsyncQueueControl.md)
  - NotificationHash
  - [Exec_ListenPreCommit](../E/Exec_ListenPreCommit.md)
  - [asyncQueueAdvance](../a/asyncQueueAdvance.md)
  - [asyncQueueAddEntries](../a/asyncQueueAddEntries.md)
  - [asyncQueueFillWarning](../a/asyncQueueFillWarning.md)
  - [SignalBackends](../S/SignalBackends.md)
  - [asyncQueueReadAllNotifications](../a/asyncQueueReadAllNotifications.md)
  - [asyncQueueProcessPageEntries](../a/asyncQueueProcessPageEntries.md)
  - [asyncQueueAdvanceTail](../a/asyncQueueAdvanceTail.md)

## Notes and Other Information
- Part of PostgreSQL's LISTEN/NOTIFY asynchronous messaging system
- Uses 64-bit page numbers to support very large queues
- Works in conjunction with SLRU (Simple Least Recently Used) buffer management
- Essential for maintaining queue cursors and tracking read/write positions
- Multiple backend processes use QueuePosition to coordinate access to the shared notification queue
- The structure is designed to be lightweight for frequent position calculations and comparisons