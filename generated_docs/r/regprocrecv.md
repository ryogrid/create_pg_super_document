# regprocrecv

## Location
src/backend/utils/adt/regproc.c: 198 - 207

## Overview
Converts external binary format data to a regproc value, serving as the binary input function for the regproc data type.

## Definition


## Detailed Description
The regprocrecv function is the binary input conversion function for PostgreSQL's regproc data type. It handles the conversion of binary data received from external sources (such as network protocols, file formats, or client libraries) into the internal regproc representation.

Since regproc values are internally stored as OIDs, this function is implemented as a simple wrapper that delegates to the standard oidrecv function. This approach ensures consistent binary format handling between regproc and OID data types while maintaining the type system distinctions at the SQL level.

## Parameters / Member Variables
- : Function call information structure containing the binary input data to be converted

## Dependencies
- Functions called/Symbols referenced:
  - : Standard OID binary input function that performs the actual conversion
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- Code sharing: Delegates entirely to oidrecv since regproc and OID share the same binary representation
- Binary protocol support: Enables regproc values to be transmitted efficiently in PostgreSQL's binary wire protocol
- Type system consistency: Maintains separate function identity while sharing implementation with OID type
- Network efficiency: Binary format is more compact than text format for network transmission and storage