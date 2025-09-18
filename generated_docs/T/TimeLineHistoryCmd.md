# TimeLineHistoryCmd

## Location
[src/include/nodes/replnodes.h:117-121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/replnodes.h#L117-L121)

## Overview
TimeLineHistoryCmd is a command structure used to request the timeline history for a specific timeline in PostgreSQL's replication system.

## Definition
```c
typedef struct TimeLineHistoryCmd
{
    NodeTag      type;
    TimeLineID   timeline;
} TimeLineHistoryCmd;
```

## Detailed Description
TimeLineHistoryCmd represents the TIMELINE_HISTORY command in PostgreSQL's streaming replication protocol. This structure is used to retrieve the complete history of a specific timeline, including information about timeline switches, branching points, and the WAL segment ranges associated with each timeline. This information is crucial for understanding the replication topology and for setting up standby servers that need to follow timeline changes.

## Parameters / Member Variables
- `type`: NodeTag identifier for this command structure  
- `timeline`: TimeLineID specifying which timeline's history to retrieve

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)
- Called from (representative examples):
  - [SendTimeLineHistory](../S/SendTimeLineHistory.md) (src/backend/replication/walsender.c:593)
  - [exec_replication_command](../e/exec_replication_command.md) (src/backend/replication/walsender.c:2173)

## Notes and Other Information
- Timeline history is essential for understanding how WAL segments relate across timeline switches
- Used by standby servers to determine which timeline to follow during recovery
- Timeline switches occur during failover scenarios when a standby server is promoted
- The returned history includes information about timeline branching points and the reasons for timeline changes
- Part of the replication protocol command set defined in replnodes.h