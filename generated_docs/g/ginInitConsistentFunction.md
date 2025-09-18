# ginInitConsistentFunction

## Location
src/backend/access/gin/ginlogic.c: 227 - 250

## Overview
Initializes and configures the appropriate consistent function implementations for a GIN scan key based on the search mode and available opclass functions.

## Definition
```c
void ginInitConsistentFunction(GinState *ginstate, GinScanKey key)
```

## Detailed Description
This function sets up the consistent function pointers for a GIN scan key by determining which implementation to use based on the search mode and the availability of opclass-provided consistent functions. It handles two main scenarios:

1. **Everything Search Mode**: When searching for all entries (GIN_SEARCH_MODE_EVERYTHING), it assigns specialized "true" consistent functions that always return positive results.

2. **Normal Search Mode**: For standard searches, it examines the ginstate to determine what consistent functions are available:
   - If a native boolean consistent function is available from the opclass, it uses the direct implementation
   - If no boolean consistent function is available, it uses a shim implementation
   - If a native tri-state consistent function is available, it uses the direct tri-consistent implementation
   - If no tri-state function is available, it uses the shimTriConsistentFn as a fallback

The function also sets up the function manager info structures and collation information needed for proper function invocation.

## Parameters / Member Variables
- `ginstate`: Pointer to GinState structure containing opclass function information and metadata for the GIN index
- `key`: Pointer to GinScanKey structure that will be configured with the appropriate consistent function implementations

## Dependencies
- Functions called/Symbols referenced:
  - trueConsistentFn
  - trueTriConsistentFn
  - directBoolConsistentFn
  - shimBoolConsistentFn
  - directTriConsistentFn
  - shimTriConsistentFn
  - GIN_SEARCH_MODE_EVERYTHING (constant)
  - OidIsValid (macro)
- Called from (representative examples):
  - ginFillScanKey
  - GinScanOpaque (indirectly through scan operations)

## Notes and Other Information
- This function is essential for GIN index scan initialization and must be called before performing consistent checks
- The function handles backward compatibility by providing shim implementations when opclasses do not provide certain consistent functions
- The choice between direct and shim implementations affects performance, with direct implementations being preferred when available
- Located at src/backend/access/gin/ginlogic.c:227-250
- The function sets up both boolean and tri-state consistent functions to support different query evaluation strategies