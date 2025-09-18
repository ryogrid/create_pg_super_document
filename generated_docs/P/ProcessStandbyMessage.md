# ProcessStandbyMessage

## Location
[src/backend/replication/walsender.c:2338-2368](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L2338-L2368)

## Overview
ProcessStandbyMessage acts as a message type dispatcher for processing status update messages received from standby servers in PostgreSQL's replication protocol.

## Definition
static void ProcessStandbyMessage(void)

## Detailed Description
ProcessStandbyMessage is a specialized message dispatcher within PostgreSQL's WAL sender that handles incoming status update messages from standby servers. The function implements a simple but critical routing mechanism for the replication feedback protocol:

1. **Message Type Extraction**: Reads the first byte of the incoming message to determine the specific type of standby message being processed.

2. **Message Routing**: Dispatches messages to appropriate specialized handlers based on the message type identifier:
   - **'r' messages**: Routes to ProcessStandbyReplyMessage() for handling standard standby reply messages that typically contain WAL position feedback
   - **'h' messages**: Routes to ProcessStandbyHSFeedbackMessage() for handling hot standby feedback messages related to transaction visibility and conflict resolution

3. **Protocol Validation**: Ensures that only recognized message types are processed, maintaining strict adherence to the PostgreSQL replication protocol.

4. **Error Handling**: Provides robust error handling for unexpected or malformed message types, treating protocol violations as fatal errors that require process termination.

The function serves as a critical component in the replication feedback loop, ensuring that standby status information is properly categorized and processed by the appropriate specialized handlers.

## Parameters / Member Variables
This function takes no parameters and operates on the global reply_message buffer that contains the incoming standby message.

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgbyte](../p/pq_getmsgbyte.md)
  - [ProcessStandbyReplyMessage](ProcessStandbyReplyMessage.md)
  - [ProcessStandbyHSFeedbackMessage](ProcessStandbyHSFeedbackMessage.md)
  - [proc_exit](../p/proc_exit.md)
  - Error reporting: COMMERROR, ERRCODE_PROTOCOL_VIOLATION
- Called from (representative examples):
  - [ProcessRepliesIfAny](ProcessRepliesIfAny.md) (at src/backend/replication/walsender.c:2294)

## Notes and Other Information
- This is a static function within walsender.c, serving as an internal dispatcher for standby message processing
- The function implements a simple switch-based dispatch pattern that can be easily extended to support additional message types
- The two currently supported message types ('r' and 'h') represent the core feedback mechanisms in PostgreSQL replication
- Protocol violations result in immediate process termination (proc_exit), reflecting the critical importance of maintaining protocol integrity
- The function assumes that the reply_message buffer has been properly populated by the calling function (typically ProcessRepliesIfAny)
- This dispatcher pattern allows for clean separation of concerns between message reception and message-specific processing logic
- The function is part of the critical path for replication feedback, making its reliability essential for proper replication operation
- Future extensions to the replication protocol would likely involve adding new cases to this dispatcher function