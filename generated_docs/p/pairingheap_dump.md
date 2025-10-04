# pairingheap_dump

## Location
[src/backend/lib/pairingheap.c:318-333](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/pairingheap.c#L318-L333)

## Overview
Creates a string representation of an entire pairing heap structure for debugging and inspection purposes.

## Definition
```c
char *pairingheap_dump(pairingheap *heap,
                      void (*dumpfunc) (pairingheap_node *node, StringInfo buf, void *opaque),
                      void *opaque)
```

## Detailed Description
This function provides a high-level interface for generating a complete textual representation of a pairing heap. It serves as the public entry point for heap debugging and introspection, coordinating the recursive traversal and formatting of the entire heap structure.

The function handles two main scenarios:
1. **Empty heap**: Returns a simple "(empty)" string for heaps with no nodes
2. **Non-empty heap**: Initializes a string buffer and delegates to `pairingheap_dump_recurse` to perform the actual tree traversal and formatting

The resulting string contains a hierarchical view of the heap where:
- Each node is represented on its own line
- Indentation indicates the depth/level in the heap tree
- Node content is formatted by the user-provided `dumpfunc` callback
- The overall structure shows parent-child relationships clearly

This function is essential for debugging heap algorithms, verifying heap properties, and understanding the internal structure during development.

## Parameters / Member Variables
- `heap`: Pointer to the pairing heap to dump
- `dumpfunc`: User-provided callback function to format individual node data
- `opaque`: User-defined data passed through to the dump function for context

## Dependencies
- Functions called/Symbols referenced:
  - [pairingheap_dump_recurse](pairingheap_dump_recurse.md) (for recursive tree traversal)
  - [pstrdup](pstrdup.md) (for empty heap string duplication)
  - [initStringInfo](../i/initStringInfo.md) (for buffer initialization)
- Called from (representative examples):
  - No direct callers found (likely used in debugging contexts)

## Notes and Other Information
- Returns a dynamically allocated string that the caller must free
- The returned string format depends entirely on the provided `dumpfunc` implementation
- Handles the edge case of empty heaps gracefully
- Not used in production PostgreSQL code paths; intended for development and debugging
- Provides complete heap visualization for troubleshooting and verification
- The function abstracts away the complexity of tree traversal from users who just want to inspect heap contents

## Simplified Source

```c
char *pairingheap_dump(pairingheap *heap,
                      void (*dumpfunc) (pairingheap_node *node, StringInfo buf, void *opaque),
                      void *opaque)
{
    // Handle empty heap case
    if (!heap->ph_root) {
        return pstrdup("(empty)");
    }

    // Create string buffer for output
    StringInfoData buf;
    initStringInfo(&buf);

    // Recursively dump the entire heap starting from root
    pairingheap_dump_recurse(&buf, heap->ph_root, dumpfunc, opaque, 0, NULL);

    return buf.data;
}
```