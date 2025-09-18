# BIG5toCNS

## Location
[src/backend/utils/mb/conversion_procs/euc_tw_and_big5/big5.c:292-344](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc_tw_and_big5/big5.c#L292-L344)

## Overview
BIG5toCNS is a public function that converts Big5 encoded characters to their corresponding CNS 11643-1992 character codes, determining the appropriate CNS plane for each converted character.

## Definition
unsigned short BIG5toCNS(unsigned short big5, unsigned char *lc)

## Detailed Description
This function performs character encoding conversion from Big5 (Traditional Chinese) to CNS 11643-1992 (Chinese National Standard). It handles multiple levels of Big5 encoding and maps them to the appropriate CNS planes. The function uses a combination of direct lookup tables for special cases and binary search for range mappings.

The conversion process involves:
1. **Level 1 characters** (big5 < 0xc940U): First checks special cases in b1c4 array for CNS Plane 4 mappings, then uses binary search on big5Level1ToCnsPlane1 for Plane 1 mappings
2. **Special case** (big5 == 0xc94aU): Direct mapping to CNS Plane 1 code 0x4442
3. **Level 2 characters** (big5 > 0xc94aU): Checks b2c3 array for Plane 3 mappings, then uses binary search on big5Level2ToCnsPlane2 for Plane 2 mappings

The function sets the appropriate locale code (lc) parameter to indicate which CNS plane the result belongs to and applies the 0x8080 mask to the output for proper formatting.

## Parameters / Member Variables
- : Input Big5 character code to be converted
- : Pointer to unsigned char that receives the CNS plane identifier (LC_CNS11643_1, LC_CNS11643_2, LC_CNS11643_3, or LC_CNS11643_4)

## Dependencies
- Functions called/Symbols referenced:
  - [BinarySearchRange](BinarySearchRange.md) (called at lines 310 and 331)
  - LC_CNS11643_1, LC_CNS11643_2, LC_CNS11643_3, LC_CNS11643_4 (locale constants)
  - b1c4 (lookup table for Big5 Level 1 to CNS Plane 4)
  - b2c3 (lookup table for Big5 Level 2 to CNS Plane 3)
  - big5Level1ToCnsPlane1 (mapping array for Level 1)
  - big5Level2ToCnsPlane2 (mapping array for Level 2)
- Called from (representative examples):
  - [big52euc_tw](../b/big52euc_tw.md) (in euc_tw_and_big5.c at line 251)
  - [big52mic](../b/big52mic.md) (in euc_tw_and_big5.c at line 482)

## Notes and Other Information
- Returns '?' character (cast to unsigned short) when no mapping is found
- Sets *lc to 0 when conversion fails
- Always applies 0x8080 mask to successful conversions for proper byte formatting
- Handles the complex multi-plane structure of CNS 11643-1992 encoding
- Critical component of PostgreSQL's multi-byte character encoding support for Traditional Chinese text