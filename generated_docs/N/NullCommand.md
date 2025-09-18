# NullCommand

## Location
src/backend/tcop/dest.c: 218 - 255

## Overview
NullCommand handles the response when an empty query string is recognized, ensuring proper protocol completion for remote clients in the extended query protocol.

## Definition
```c
void NullCommand(CommandDest dest)
```

## Detailed Description
NullCommand provides the appropriate response when PostgreSQL receives an empty query string. This function is crucial for maintaining proper client-server communication, especially in the extended query protocol where clients expect a recognizable end to the response for Execute messages. For remote destinations, it sends an EmptyQueryResponse message to notify the frontend that an empty query was processed. For local destinations, no special handling is required.

The function ensures protocol compliance by providing a definitive response that clients can recognize, preventing them from waiting indefinitely for a response that would never come.

## Parameters / Member Variables
- `dest`: CommandDest enumeration value specifying where the command output should be directed

## Dependencies
- Functions called/Symbols referenced:
  - pq_putemptymessage (sends empty message to client)
  - PqMsg_EmptyQueryResponse (protocol message type for empty queries)
  - CommandDest enum values (DestRemote, DestRemoteExecute, etc.)
- Called from (representative examples):
  - exec_simple_query (in postgres.c)
  - exec_execute_message (in postgres.c)

## Notes and Other Information
- Located in src/backend/tcop/dest.c:218-255
- Essential for extended query protocol compliance
- Only remote destinations require empty query response handling
- Prevents client timeout issues when empty queries are submitted
- Works alongside PostgreSQL's query processing infrastructure to handle edge cases
- Part of the command completion framework but specifically handles the 'no command' case
- Ensures that every query submission receives some form of response from the server