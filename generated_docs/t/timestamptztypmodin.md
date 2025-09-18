# timestamptztypmodin

## Location
src/backend/utils/adt/timestamp.c: 858 - 865

## Overview
Processes and validates type modifier input for the timestamptz data type, converting string array input to internal typmod representation.

## Definition


## Detailed Description
The  function serves as the type modifier input function for the timestamptz data type. It processes an array of strings containing type modifier specifications (such as precision values) and converts them into PostgreSQL's internal typmod representation. This function is called during SQL parsing when timestamptz types are declared with modifiers, such as  for 3-digit fractional seconds precision.

The function delegates the actual parsing and validation logic to , passing  to indicate this is for a timestamptz type (with timezone) rather than a plain timestamp type.

## Parameters / Member Variables
- : ArrayType pointer containing the string array of type modifier specifications (from )

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract ArrayType argument
  - : Shared function that handles typmod parsing for timestamp types
  - : Macro to return the processed typmod as an integer
- Called from (representative examples):
  - No direct references found (used internally by PostgreSQL's type system during SQL parsing)

## Notes and Other Information
- Located in src/backend/utils/adt/timestamp.c:858-865
- Part of PostgreSQL's type system infrastructure for handling type modifiers
- The function is a thin wrapper around  with the timezone flag set to true
- Used during SQL parsing when timestamptz types are declared with precision specifiers
- The resulting typmod value is used by other functions like 
- Type modifiers for timestamptz typically specify fractional seconds precision (0-6)
- Called automatically by PostgreSQL's parser infrastructure, not by user code directly