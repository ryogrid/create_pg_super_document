# readBitmapset

## Location
src/backend/nodes/readfuncs.c: 245 - 258

## Overview
A public wrapper function that provides external access to Bitmapset deserialization functionality, primarily for use by PostgreSQL extensions that define extensible nodes.

## Definition
```c
Bitmapset *readBitmapset(void)
```

## Detailed Description
The `readBitmapset` function serves as a public interface to the internal `_readBitmapset` functionality. It was originally designed to allow PostgreSQL extensions to deserialize Bitmapset structures when implementing custom extensible nodes. However, as noted in the code comments, this function has become somewhat historical since the general `nodeRead()` function can now handle Bitmapset deserialization in most contexts.

The function exists primarily for backward compatibility and to maintain a stable API for extensions that may still rely on explicit Bitmapset reading capabilities. It simply delegates all work to the internal `_readBitmapset` function, providing no additional processing or validation.

## Parameters / Member Variables
- No parameters (inherits tokenization state from global context)
- Returns: `Bitmapset *` - pointer to the deserialized Bitmapset structure

## Dependencies
- Functions called/Symbols referenced:
  - `_readBitmapset` (internal implementation function)
- Called from (representative examples):
  - Currently no direct callers in the core PostgreSQL codebase
  - Intended for use by external extensions

## Notes and Other Information
- This is a public function (non-static), exported for use by extensions
- Serves as a thin wrapper around `_readBitmapset` with no additional functionality
- Marked as "somewhat historical" in the source comments, indicating its reduced relevance in modern PostgreSQL
- The comment suggests that `nodeRead()` is now the preferred method for general-purpose node deserialization, including Bitmapsets
- Maintained for API compatibility with existing extensions
- Part of the broader extensibility framework that allows third-party code to integrate with PostgreSQL's node system