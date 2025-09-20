# _hash_dropscanbuf

## Location
[src/backend/access/hash/hashpage.c:289-326](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashpage.c#L289-L326)

## Overview
This function releases all buffers held during a hash index scan operation, properly cleaning up buffer references and resetting scan state to prevent resource leaks.

## Definition

```c
void
_hash_dropscanbuf(Relation rel, HashScanOpaque so)
```
## Detailed Description
 is a comprehensive cleanup function that releases all buffers associated with a hash index scan operation. It handles three categories of buffers: the primary bucket buffer, the split bucket buffer (used during bucket splits), and the current position buffer. The function carefully avoids double-releasing buffers by checking if different buffer variables point to the same underlying buffer. After releasing all buffer pins, it resets the scan state flags to indicate that no bucket data is currently loaded.

## Parameters / Member Variables
- : The relation (hash index) being scanned
- : The hash scan opaque structure containing scan state and buffer references

## Dependencies
- Functions called/Symbols referenced:
  - [BufferIsValid](../B/BufferIsValid.md) (buffer validity check macro)
  - [_hash_dropbuf](_hash_dropbuf.md) (buffer release function)
  - InvalidBuffer (invalid buffer constant)
  - HashScanOpaque (scan state structure type)

- Called from (representative examples):
  - [hashrescan](hashrescan.md) (scan restart operations)
  - [hashendscan](hashendscan.md) (scan termination)
  - [_hash_next](_hash_next.md) (scan advancement with cleanup)

## Notes and Other Information
- The function handles three types of buffers maintained during hash scans:
  - : Buffer for the primary bucket being scanned
  - : Buffer for the bucket being split (during concurrent splits)
  - : Buffer for the current scan position
- Includes protection against double-release by checking if buffers are the same before releasing
- Resets scan state flags ( and ) to indicate clean state
- Essential for proper resource management in hash index scans, especially during error recovery
- The buffer comparison logic prevents releasing the same buffer multiple times when scan state variables point to the same underlying buffer
- Used both for normal scan termination and for rescan operations where scan state needs to be reset