# reindex_error_callback

## Location
src/backend/commands/indexcmds.c: 3196 - 3216

## Overview
reindex_error_callback provides contextual error information when reindexing operations on partitioned tables or indexes encounter errors.

## Definition
```c
static void reindex_error_callback(void *arg)
```

## Detailed Description
This function serves as an error callback specifically designed for ReindexPartitions() operations. It enhances error reporting by providing meaningful context when reindexing operations fail on partitioned relations. The function:

1. **Type Validation**: Asserts that the relation being processed is indeed a partitioned relation (using RELKIND_HAS_PARTITIONS macro)
2. **Context Generation**: Generates appropriate error context messages based on the specific type of partitioned relation:
   - For partitioned tables: Provides context indicating reindexing of a partitioned table
   - For partitioned indexes: Provides context indicating reindexing of a partitioned index
3. **Error Enhancement**: Uses errcontext() to add contextual information to error messages, including the qualified name (schema.relation) of the problematic relation

## Parameters / Member Variables
- `arg`: Void pointer to ReindexErrorInfo structure containing error context information including relation kind, namespace, and name

## Dependencies
- Functions called/Symbols referenced:
  - RELKIND_HAS_PARTITIONS (macro)
  - errcontext
  - RELKIND_PARTITIONED_INDEX
- Called from:
  - [ReindexPartitions](../R/ReindexPartitions.md)

## Notes and Other Information
- This callback is registered with the PostgreSQL error handling system to provide enhanced error messages during partition reindexing operations
- The function assumes the ReindexErrorInfo structure contains valid relation metadata including relkind, relnamespace, and relname
- Error context messages include both schema and relation names for precise identification of the problematic relation
- The function only handles partitioned tables and partitioned indexes, as asserted by the RELKIND_HAS_PARTITIONS check
- Provides user-friendly error messages that help identify exactly which partitioned relation was being processed when an error occurred