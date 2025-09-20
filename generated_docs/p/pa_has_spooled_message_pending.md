# pa_has_spooled_message_pending

## Location
[src/backend/replication/logical/applyparallelworker.c:642-657](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/applyparallelworker.c#L642-L657)

## Overview
Checks if there are any pending spooled messages by examining the current file set state to determine if it's empty.

## Definition
```c
static bool pa_has_spooled_message_pending()
```

## Detailed Description
This function provides a simple check to determine whether there are any spooled messages waiting to be processed. It accomplishes this by querying the current state of the file set used for message spooling through pa_get_fileset_state() and comparing it against the FS_EMPTY state. If the file set is not empty, it indicates that there are pending spooled messages that need to be processed.

This function is typically used in scenarios where the system needs to decide whether to wait for more messages or proceed with processing existing spooled data.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [pa_get_fileset_state](pa_get_fileset_state.md) (get current fileset state)
  - PartialFileSetState (enum type for fileset states)
  - FS_EMPTY (constant indicating empty fileset)
- Called from (representative examples):
  - [pa_decr_and_wait_stream_block](pa_decr_and_wait_stream_block.md)

## Notes and Other Information
- Static function indicating internal use within the applyparallelworker module
- Simple boolean check that abstracts the fileset state examination
- Used as part of the message spooling infrastructure for parallel apply workers
- Return value indicates whether spooled message processing is needed