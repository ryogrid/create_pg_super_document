heap_identify

## Overview
This function converts heap WAL record operation codes into human-readable string identifiers for debugging and logging purposes.

## Definition
const char *heap_identify(uint8 info)

## Detailed Description
heap_identify is a utility function that maps heap-related WAL record operation codes to descriptive string names. It processes the info parameter by masking off non-operation bits using XLR_INFO_MASK and then uses a switch statement to identify the specific heap operation type.

The function handles all major heap operations including:
- INSERT operations (with optional page initialization)
- DELETE operations  
- UPDATE operations (regular and HOT updates, with optional page initialization)
- TRUNCATE operations
- CONFIRM operations for speculative insertions
- LOCK operations for tuple locking
- INPLACE operations for in-place updates

This function is essential for PostgreSQL debugging tools, log analysis, and WAL record inspection utilities.

## Parameters / Member Variables
- info: 8-bit unsigned integer containing the WAL record operation code and flags

## Dependencies
- Functions called/Symbols referenced:
  - XLR_INFO_MASK (constant for masking info bits)
  - XLOG_HEAP_INSERT
  - XLOG_HEAP_INIT_PAGE  
  - XLOG_HEAP_DELETE
  - XLOG_HEAP_UPDATE
  - XLOG_HEAP_HOT_UPDATE
  - XLOG_HEAP_TRUNCATE
  - XLOG_HEAP_CONFIRM
  - XLOG_HEAP_LOCK
  - XLOG_HEAP_INPLACE
- Called from (representative examples):
  - WAL record identification infrastructure (indirectly through resource manager tables)

## Notes and Other Information
- Returns NULL for unrecognized operation codes
- The function combines operation codes with XLOG_HEAP_INIT_PAGE flag to provide more specific identification
- HOT (Heap-Only Tuple) updates are distinguished from regular updates
- This function is part of the heap resource manager description system
- Located in src/backend/access/rmgrdesc/heapdesc.c:385-429

## Simplified Source

```c
const char *heap_identify(uint8 info) {
    const char *id = NULL;

    // Map operation code to readable string
    switch (info & ~XLR_INFO_MASK) {
        case XLOG_HEAP_INSERT:
            id = "INSERT";
            break;
        case XLOG_HEAP_INSERT | XLOG_HEAP_INIT_PAGE:
            id = "INSERT+INIT";
            break;
        case XLOG_HEAP_DELETE:
            id = "DELETE";
            break;
        case XLOG_HEAP_UPDATE:
            id = "UPDATE";
            break;
        case XLOG_HEAP_UPDATE | XLOG_HEAP_INIT_PAGE:
            id = "UPDATE+INIT";
            break;
        case XLOG_HEAP_HOT_UPDATE:
            id = "HOT_UPDATE";
            break;
        case XLOG_HEAP_HOT_UPDATE | XLOG_HEAP_INIT_PAGE:
            id = "HOT_UPDATE+INIT";
            break;
        case XLOG_HEAP_TRUNCATE:
            id = "TRUNCATE";
            break;
        case XLOG_HEAP_CONFIRM:
            id = "HEAP_CONFIRM";
            break;
        case XLOG_HEAP_LOCK:
            id = "LOCK";
            break;
        case XLOG_HEAP_INPLACE:
            id = "INPLACE";
            break;
    }

    return id;
}
```