# ReindexErrorInfo

## Location
[src/backend/commands/indexcmds.c:132-137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/indexcmds.c#L132-L137)

## Overview
A structure that holds error context information for error callbacks during partitioned table and index reindexing operations.

## Definition


## Detailed Description
The  structure is specifically designed to provide contextual information for error reporting during reindexing operations on partitioned tables and indexes. It is used as an argument to the  function, which formats error messages with appropriate context about which partitioned relation was being processed when an error occurred.

This structure enables more informative error messages by preserving the name, namespace, and kind of the relation being reindexed, allowing users to quickly identify the source of reindexing failures in complex partitioned table hierarchies.

## Parameters / Member Variables
- : The name of the relation (table or index) being reindexed
- : The namespace (schema) name of the relation
- : The kind of relation (RELKIND_PARTITIONED_TABLE or RELKIND_PARTITIONED_INDEX)

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a plain data structure)
- Called from (representative examples):
  - [reindex_error_callback](../r/reindex_error_callback.md)
  - [ReindexPartitions](ReindexPartitions.md)

## Notes and Other Information
- This structure is only used for partitioned relations (tables and indexes) as indicated by the assertion in the error callback
- The error callback uses this information to generate context-appropriate error messages
- Memory management for the string fields is handled by the calling context
- Part of the internal error handling mechanism for partition reindexing operations