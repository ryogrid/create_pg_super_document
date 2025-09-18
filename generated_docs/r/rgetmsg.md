# rgetmsg

## Location
src/interfaces/ecpg/compatlib/informix.c: 986 - 994

## Overview
The rgetmsg function is a stub implementation that provides Informix compatibility for message retrieval functionality, but currently returns a no-op result.

## Definition


## Detailed Description
The rgetmsg function is part of PostgreSQL's ECPG Informix compatibility library. This is a stub implementation that mimics the signature of the Informix rgetmsg function, which would typically retrieve system or error messages by message number. However, the PostgreSQL implementation simply ignores all parameters and returns 0, indicating no message was retrieved.

The function parameters are explicitly cast to void to prevent compiler warnings about unused parameters, indicating this is intentionally a no-op implementation for compatibility purposes.

## Parameters / Member Variables
- : Message number to retrieve (ignored in current implementation)
- : Buffer to store the retrieved message (ignored in current implementation)
- : Maximum size of the message buffer (ignored in current implementation)

## Dependencies
- Functions called/Symbols referenced:
  - None (only uses void casts for compiler quieting)
- Called from (representative examples):
  - ECPG_INFORMIX_EXTRA_CHARS macro context

## Notes and Other Information
- This is a stub implementation - no actual message retrieval occurs
- Always returns 0, typically indicating no message or failure to retrieve
- All parameters are intentionally ignored with void casts to suppress compiler warnings
- Provides API compatibility with Informix applications that use rgetmsg()
- Located in src/interfaces/ecpg/compatlib/informix.c:986-994
- May require actual implementation in the future if full Informix message compatibility is needed