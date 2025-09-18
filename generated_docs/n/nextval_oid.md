# nextval_oid

## Location
src/backend/commands/sequence.c: 615 - 622

## Overview
Provides the PostgreSQL SQL function interface for obtaining the next value from a sequence using the sequence's OID as the identifier.

## Definition
```c
Datum nextval_oid(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements a streamlined version of the nextval() functionality that takes a sequence relation OID directly as input instead of parsing a text sequence name. It provides a more efficient path for sequence value generation when the sequence OID is already known, bypassing the overhead of name resolution and parsing that is required by the text-based nextval() function.

The function serves as a thin wrapper around nextval_internal(), directly passing the provided OID and requesting sequence checking. This makes it particularly useful for internal PostgreSQL operations and optimized code paths where the sequence OID is readily available.

## Parameters / Member Variables
- Function argument accessed via PG_GETARG_OID(0): The OID of the sequence relation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID (to get OID argument)
  - nextval_internal
  - PG_RETURN_INT64
- Called from (representative examples):
  - No current references found in the analyzed codebase

## Notes and Other Information
- More efficient than text-based nextval() since it avoids name parsing and resolution
- Directly uses OID input, making it suitable for internal operations where sequence OID is known
- Passes 'true' for the check parameter to nextval_internal, enabling sequence validation
- Returns INT64 (bigint) values to support the full range of sequence values
- Part of the modern OID-based sequence interface alongside the legacy text-based approach
- Currently appears to be available but not actively used in the analyzed codebase portions