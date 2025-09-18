# ReorderBufferExecuteInvalidations

## Location
src/backend/replication/logical/reorderbuffer.c: 3518 - 3529

## Overview
Executes all accumulated invalidation messages during logical replication replay to maintain cache consistency.

## Definition
static void ReorderBufferExecuteInvalidations(uint32 nmsgs, SharedInvalidationMessage *msgs)

## Detailed Description
This function applies all invalidation messages that have been collected during logical replication processing. Invalidation messages are used to maintain cache consistency across PostgreSQL's shared catalog caches. When logical replication replays changes, it needs to ensure that any cached catalog information is properly invalidated to reflect the changes being applied. The function iterates through all provided invalidation messages and executes each one locally.

## Parameters / Member Variables
- nmsgs: The number of invalidation messages to process
- msgs: Array of SharedInvalidationMessage structures containing the invalidation messages to execute

## Dependencies
- Functions called/Symbols referenced:
  - [LocalExecuteInvalidationMessage](../L/LocalExecuteInvalidationMessage.md)
  - SharedInvalidationMessage
- Called from (representative examples):
  - [ReorderBufferProcessTXN](ReorderBufferProcessTXN.md)
  - [ReorderBufferFinishPrepared](ReorderBufferFinishPrepared.md)

## Notes and Other Information
- This is a static function, only accessible within the reorderbuffer.c file
- The function applies all invalidations without selective filtering, as noted in the comment that it may only need parts at the current point in the changestream but doesn't know which ones
- Essential for maintaining catalog cache consistency during logical replication replay
- Part of PostgreSQL's logical replication infrastructure