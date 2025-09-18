# fmStringInfo

## Location
src/include/fmgr.h: 29 - 37

## Overview
fmStringInfo is a typedef that represents a pointer to a StringInfoData structure, used as a stub reference in the function manager system to avoid including stringinfo.h.

## Definition


## Detailed Description
fmStringInfo is a forward declaration typedef defined in fmgr.h that creates a pointer type to the StringInfoData structure without requiring the full definition from stringinfo.h. The StringInfoData structure is PostgreSQL's primary string buffer implementation, providing dynamic string building capabilities with automatic memory management. By using this typedef, the function manager can reference string buffers in function signatures and contexts without exposing the complete StringInfoData implementation, maintaining header dependency separation and reducing compilation overhead.

## Parameters / Member Variables
- This is a simple typedef with no parameters or member variables

## Dependencies
- Functions called/Symbols referenced:
  - StringInfoData (struct - forward declaration only)
- Called from (representative examples):
  - OidFunctionCall9 (function call interface)

## Notes and Other Information
- This typedef serves as an abstraction layer to avoid including stringinfo.h in fmgr.h
- The actual StringInfoData structure definition is found in lib/stringinfo.h
- Used in function interfaces that need to pass or return dynamic string buffers
- Part of PostgreSQL's modular header design to minimize compilation dependencies
- Enables string buffer operations in function manager contexts without full header inclusion
- Commonly used for output parameters and string building operations in PostgreSQL functions