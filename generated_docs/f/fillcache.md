# fillcache

## Location
[src/backend/utils/adt/tsgistidx.c:576-586](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsgistidx.c#L576-L586)

## Overview
Populates a cache structure with signature information from a tsvector key, handling array keys, ALLTRUE signatures, and regular bit vector signatures.

## Definition
```c
static void fillcache(CACHESIGN *item, SignTSVector *key, int siglen)
```

## Detailed Description
The `fillcache` function initializes a `CACHESIGN` cache item with signature data extracted from a text search vector key. This function serves as a preprocessing step during GiST index operations, particularly in the picksplit process, where multiple signatures need to be repeatedly accessed and compared.

The function handles three types of keys:
1. **Array keys (ISARRKEY)**: Converts the array representation to a signature using `makesign`
2. **ALLTRUE keys**: Sets the allistrue flag instead of copying signature data
3. **Regular signature keys**: Directly copies the signature bit vector

This caching mechanism optimizes performance by avoiding repeated conversions and providing a unified interface for signature comparison operations.

## Parameters / Member Variables
- `item`: Pointer to CACHESIGN structure to be filled
- `key`: Source SignTSVector key containing the signature data
- `siglen`: Length of the signature in bytes

## Dependencies
- Functions called/Symbols referenced:
  - ISARRKEY (macro to check if key is array-based)
  - [makesign](../m/makesign.md) (function to create signature from array)
  - ISALLTRUE (macro to check if signature is in ALLTRUE state)
  - GETSIGN (macro to get signature bit vector)
  - memcpy (standard memory copy function)
- Called from:
  - [gtsvector_picksplit](../g/gtsvector_picksplit.md) (multiple times during index node splitting)

## Notes and Other Information
- This is a static helper function used only within tsgistidx.c
- The CACHESIGN structure contains an `allistrue` boolean flag and a `sign` bit vector pointer
- Used to optimize signature access during expensive GiST operations like node splitting
- Always initializes `allistrue` to false first, then sets it to true only for ALLTRUE keys
- The cache assumes that the `sign` member has been pre-allocated with sufficient space
- Located in src/backend/utils/adt/tsgistidx.c:576-586

## Simplified Source

```c
static void fillcache(CACHESIGN *item, SignTSVector *key, int siglen) {
    item->allistrue = false;

    if (ISARRKEY(key)) {
        // Convert array key to signature format
        makesign(item->sign, key, siglen);
    }
    else if (ISALLTRUE(key)) {
        // Mark as ALLTRUE signature (no signature data needed)
        item->allistrue = true;
    }
    else {
        // Copy existing signature data
        memcpy(item->sign, GETSIGN(key), siglen);
    }
}
```