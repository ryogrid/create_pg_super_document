# RewriteMappingFile

## Location
[src/backend/access/heap/rewriteheap.c:191-198](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/rewriteheap.c#L191-L198)

## Overview
RewriteMappingFile represents in-memory metadata for managing logical remapping entries during heap rewrite operations, specifically tracking mappings for transactions that may need to access rewritten data.

## Definition
```c
typedef struct RewriteMappingFile
{
    TransactionId xid;          /* xid that might need to see the row */
    int           vfd;          /* fd of mappings file */
    off_t         off;          /* how far have we written yet */
    dclist_head   mappings;     /* list of in-memory mappings */
    char          path[MAXPGPATH]; /* path, for error messages */
} RewriteMappingFile;
```

## Detailed Description
RewriteMappingFile serves as a control structure for managing logical mapping files during heap rewrite operations. These files are essential for maintaining consistency in logical replication scenarios where concurrent transactions need to map old tuple locations to new ones after a rewrite. The structure maintains both in-memory mappings for performance and persistent file storage for durability. Each instance is associated with a specific transaction ID that represents transactions potentially needing access to the mapping information.

## Parameters / Member Variables
- `xid`: Transaction ID that might need to access the rewritten row mappings
- `vfd`: Virtual file descriptor for the mappings file on disk
- `off`: Current write offset position in the mappings file
- `mappings`: Doubly-linked list head for in-memory mapping entries
- `path`: Full file system path to the mappings file (used for error reporting)

## Dependencies
- Functions called/Symbols referenced:
  - vfd (virtual file descriptor type)
  - [dclist_head](../d/dclist_head.md) (doubly-linked circular list header)
- Called from (representative examples):
  - [logical_begin_heap_rewrite](../l/logical_begin_heap_rewrite.md)
  - [logical_heap_rewrite_flush_mappings](../l/logical_heap_rewrite_flush_mappings.md)
  - [logical_end_heap_rewrite](../l/logical_end_heap_rewrite.md)
  - [logical_rewrite_log_mapping](../l/logical_rewrite_log_mapping.md)
  - [UpdateLogicalMappings](../U/UpdateLogicalMappings.md)

## Notes and Other Information
This structure is primarily used in logical replication contexts where maintaining tuple mapping consistency across rewrite operations is critical. The combination of in-memory and on-disk storage provides both performance optimization and crash recovery capabilities. The file-based approach allows the system to handle large rewrite operations that exceed available memory while ensuring mapping information persists across potential system failures.