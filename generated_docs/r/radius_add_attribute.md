# radius_add_attribute

## Location
[src/backend/libpq/auth.c:2821-2846](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth.c#L2821-L2846)

## Overview
Adds a RADIUS attribute to a RADIUS packet structure, ensuring the packet doesn't exceed buffer size limits.

## Definition


## Detailed Description
This static function is responsible for safely adding RADIUS attributes to a RADIUS packet during the authentication process. It performs bounds checking to ensure that adding the new attribute won't cause the packet to exceed the maximum RADIUS buffer size (RADIUS_BUFFER_SIZE). The function constructs a properly formatted RADIUS attribute with the correct type, length, and data fields, then appends it to the existing packet structure.

If adding the attribute would cause a buffer overflow, the function logs a warning and skips adding the attribute rather than corrupting memory. This defensive approach ensures that authentication will fail gracefully rather than causing a security vulnerability or system crash.

## Parameters / Member Variables
- : Pointer to the RADIUS packet structure to which the attribute will be added
- : The RADIUS attribute type code (uint8) identifying what kind of attribute this is
- : Pointer to the raw data content for the attribute
- : Length in bytes of the data to be added (not including the attribute header)

## Dependencies
- Functions called/Symbols referenced:
  - elog (for warning messages)
  - memcpy (for copying attribute data)
- Types referenced:
  - radius_packet
  - [radius_attribute](radius_attribute.md)
  - RADIUS_BUFFER_SIZE (constant)
- Called from:
  - [PerformRadiusTransaction](../P/PerformRadiusTransaction.md) (multiple times at lines 3004, 3005, 3006, 3053)

## Notes and Other Information
- This is a static function, only visible within the auth.c compilation unit
- The function includes robust bounds checking to prevent buffer overflows
- Attribute length includes both the data length plus 2 bytes for the type and length fields themselves
- If buffer overflow would occur, the attribute is silently dropped with only a warning logged
- The function directly manipulates the packet buffer memory layout to append the new attribute
- Part of PostgreSQL's RADIUS authentication implementation for external authentication servers