# logicalrep_message_type

## Location
[src/backend/replication/logical/proto.c:1217-1271](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L1217-L1271)

## Overview
A utility function that converts LogicalRepMsgType enumeration values to their corresponding string representations for debugging and error reporting purposes.

## Definition

```c
const char *
logicalrep_message_type(LogicalRepMsgType action)
```
## Detailed Description
The  function serves as a mapping utility in PostgreSQL's logical replication system. It takes a  enumeration value and returns a human-readable string representation of the message type. This function is particularly useful for error reporting, debugging, and logging within the logical replication framework.

The function implements a comprehensive switch statement that covers all possible logical replication message types, including standard operations (BEGIN, COMMIT, INSERT, UPDATE, DELETE), streaming operations, and prepared transaction operations. When an unknown message type is encountered, the function gracefully handles it by returning a formatted string indicating the unknown type code rather than throwing an error.

This design choice is intentional - since the function is often used in error reporting contexts, throwing an error would mask the original error that triggered the call.

## Parameters / Member Variables
- `action`: A  enumeration value representing the type of logical replication message to convert to a string
## Dependencies
- Functions called/Symbols referenced:
  -  (enumeration type)
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  -  (for formatting unknown message types)
- Called from (representative examples):
  -  (in src/backend/replication/logical/worker.c)

## Notes and Other Information
- The function uses a static local buffer  to store the formatted string for unknown message types, ensuring thread safety for the error case
- Returns string literals for known message types, making it memory-efficient
- Designed to never throw errors since it's primarily used in error reporting contexts
- Supports all logical replication message types including streaming and prepared transaction operations
- The function is located in src/backend/replication/logical/proto.c:1217-1271

## Simplified Source

```c
const char *logicalrep_message_type(LogicalRepMsgType action) {
    static char err_unknown[20];

    // Map message type enum to human-readable string
    switch (action) {
        case LOGICAL_REP_MSG_BEGIN:            return "BEGIN";
        case LOGICAL_REP_MSG_COMMIT:           return "COMMIT";
        case LOGICAL_REP_MSG_ORIGIN:           return "ORIGIN";
        case LOGICAL_REP_MSG_INSERT:           return "INSERT";
        case LOGICAL_REP_MSG_UPDATE:           return "UPDATE";
        case LOGICAL_REP_MSG_DELETE:           return "DELETE";
        case LOGICAL_REP_MSG_TRUNCATE:         return "TRUNCATE";
        case LOGICAL_REP_MSG_RELATION:         return "RELATION";
        case LOGICAL_REP_MSG_TYPE:             return "TYPE";
        case LOGICAL_REP_MSG_MESSAGE:          return "MESSAGE";
        case LOGICAL_REP_MSG_BEGIN_PREPARE:    return "BEGIN PREPARE";
        case LOGICAL_REP_MSG_PREPARE:          return "PREPARE";
        case LOGICAL_REP_MSG_COMMIT_PREPARED:  return "COMMIT PREPARED";
        case LOGICAL_REP_MSG_ROLLBACK_PREPARED: return "ROLLBACK PREPARED";
        case LOGICAL_REP_MSG_STREAM_START:     return "STREAM START";
        case LOGICAL_REP_MSG_STREAM_STOP:      return "STREAM STOP";
        case LOGICAL_REP_MSG_STREAM_COMMIT:    return "STREAM COMMIT";
        case LOGICAL_REP_MSG_STREAM_ABORT:     return "STREAM ABORT";
        case LOGICAL_REP_MSG_STREAM_PREPARE:   return "STREAM PREPARE";
    }

    // Handle unknown message types gracefully (for error reporting)
    snprintf(err_unknown, sizeof(err_unknown), "??? (%d)", action);
    return err_unknown;
}
```