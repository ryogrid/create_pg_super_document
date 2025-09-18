# xl_logical_message

## Location
[src/include/replication/message.h:20-28](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/message.h#L20-L28)

## Overview
The  structure represents a generic logical decoding message WAL record used for storing custom application messages in the PostgreSQL Write-Ahead Log (WAL) for logical replication.

## Definition


## Detailed Description
The  structure is designed to store application-defined logical messages in PostgreSQL's WAL stream. These messages can be either transactional (committed with a transaction) or non-transactional (immediately visible). The structure provides a flexible way for applications to insert custom data into the logical replication stream that can be consumed by logical decoding output plugins.

The structure uses a flexible array member to store both the prefix (a null-terminated string identifier) and the actual message payload in a single contiguous memory block. During logical decoding, these messages are processed by the  function and can be filtered by database ID and replication origin.

## Parameters / Member Variables
- : The database OID from which the message was emitted, used for filtering messages during logical decoding
- : Boolean flag indicating whether the message is part of a transaction (true) or should be processed immediately (false)
- : The length of the null-terminated prefix string, including the null terminator
- : The size in bytes of the actual message payload data
- : A flexible array member containing the concatenated prefix string and message payload data

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (macro for flexible array member declaration)
- Called from (representative examples):
  - [logicalmsg_decode](../l/logicalmsg_decode.md) (decode.c:606, 617)
  - [LogLogicalMessage](../L/LogLogicalMessage.md) (message.c:46)
  - [logicalmsg_desc](../l/logicalmsg_desc.md) (logicalmsgdesc.c:26)
  - SizeOfLogicalMessage (message.h:30)

## Notes and Other Information
- The  field contains both the prefix and the actual message data concatenated together, with the prefix being null-terminated
- The  macro calculates the size of the fixed part of the structure (excluding the flexible array member)
- Messages are filtered during logical decoding based on database ID and replication origin
- Non-transactional messages are processed immediately during decoding, while transactional messages are queued until the transaction commits
- The structure is used in conjunction with the RM_LOGICALMSG_ID resource manager for WAL record processing
- Applications can use the  SQL function to generate these WAL records