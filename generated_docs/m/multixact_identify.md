# multixact_identify

## Location
[src/backend/access/rmgrdesc/mxactdesc.c:84-105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/mxactdesc.c#L84-L105)

## Overview
A WAL record identification function that returns human-readable string names for different types of multixact-related transaction log operations.

## Definition
```c
const char *multixact_identify(uint8 info)
```

## Detailed Description
The `multixact_identify` function is part of PostgreSQL's WAL record identification system. It takes a numeric operation code from a WAL record and returns a corresponding human-readable string name. This function supports the WAL analysis and debugging infrastructure by providing meaningful names for multixact operations.

The function handles four types of multixact operations:
- **ZERO_OFF_PAGE**: Returns "ZERO_OFF_PAGE" for offset page zeroing operations
- **ZERO_MEM_PAGE**: Returns "ZERO_MEM_PAGE" for member page zeroing operations  
- **CREATE_ID**: Returns "CREATE_ID" for multixact creation operations
- **TRUNCATE_ID**: Returns "TRUNCATE_ID" for multixact truncation operations

If an unknown operation code is provided, the function returns NULL.

## Parameters / Member Variables
- `info`: 8-bit unsigned integer containing the WAL record operation code to identify

## Dependencies
- Functions called/Symbols referenced:
  - XLR_INFO_MASK
  - XLOG_MULTIXACT_ZERO_OFF_PAGE
  - XLOG_MULTIXACT_ZERO_MEM_PAGE
  - XLOG_MULTIXACT_CREATE_ID
  - XLOG_MULTIXACT_TRUNCATE_ID
- Called from (representative examples):
  - WAL analysis tools (referenced in SizeOfMultiXactTruncate)

## Notes and Other Information
- Returns NULL for unrecognized operation codes rather than a default string
- Uses bit masking with XLR_INFO_MASK to extract the actual operation type, filtering out additional WAL record flags
- Part of the resource manager identification interface for multixact operations
- Essential for WAL dump utilities and debugging tools that need to display operation names
- Complements the multixact_desc function by providing operation type identification

## Simplified Source

```c
const char *multixact_identify(uint8 info) {
    // Extract operation type from WAL record info
    switch (info & ~XLR_INFO_MASK) {
        case XLOG_MULTIXACT_ZERO_OFF_PAGE:
            return "ZERO_OFF_PAGE";
        case XLOG_MULTIXACT_ZERO_MEM_PAGE:
            return "ZERO_MEM_PAGE";
        case XLOG_MULTIXACT_CREATE_ID:
            return "CREATE_ID";
        case XLOG_MULTIXACT_TRUNCATE_ID:
            return "TRUNCATE_ID";
    }
    return NULL;  // Unknown operation type
}
```