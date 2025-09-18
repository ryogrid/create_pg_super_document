# radius_attribute

## Location
src/backend/libpq/auth.c: 2791 - 2800

## Overview
The  structure represents individual RADIUS attributes within RADIUS authentication packets, as defined in RFC2865 for PostgreSQL's RADIUS authentication implementation.

## Definition


## Detailed Description
The  structure is a fundamental component of PostgreSQL's RADIUS authentication system, defined in . This structure represents the standard RADIUS attribute format as specified in RFC2865. Each attribute consists of a type identifier, length field, and variable-length data payload. The structure uses a flexible array member for the data field, allowing it to accommodate attributes of varying sizes while maintaining memory efficiency. This design follows the RADIUS protocol specification where attributes are variable-length and packed sequentially within RADIUS packets.

## Parameters / Member Variables
- : An 8-bit identifier specifying the type of RADIUS attribute (e.g., RADIUS_USER_NAME, RADIUS_USER_PASSWORD)
- : An 8-bit field indicating the total length of the attribute including the attribute and length fields themselves (minimum value is 2)
- : A flexible array member containing the actual attribute data, with length determined by the length field minus 2

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (macro for flexible array declaration)
- Called from (representative examples):
  - [radius_add_attribute](radius_add_attribute.md) (uses radius_attribute pointer to construct attributes within RADIUS packets)

## Notes and Other Information
- The structure is designed to be packed sequentially within RADIUS packets without padding
- The length field includes the 2-byte overhead for the attribute and length fields themselves
- This structure is used exclusively within the RADIUS authentication context and is not exposed outside the authentication subsystem
- The flexible array member design allows for efficient memory usage when dealing with variable-length RADIUS attributes
- Attributes are added to RADIUS packets using the radius_add_attribute function, which performs bounds checking to prevent buffer overruns