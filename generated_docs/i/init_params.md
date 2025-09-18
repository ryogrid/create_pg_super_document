# init_params

## Location
src/backend/commands/sequence.c: 1257 - 1592

## Overview
Processes CREATE or ALTER SEQUENCE option lists and validates/stores sequence parameters in catalog and data structures.

## Definition
```c
static void init_params(ParseState *pstate, List *options, bool for_identity,
                       bool isInit,
                       Form_pg_sequence seqform,
                       Form_pg_sequence_data seqdataform,
                       bool *need_seq_rewrite,
                       List **owned_by)
```

## Detailed Description
This comprehensive function handles the parsing, validation, and initialization of sequence parameters from SQL CREATE SEQUENCE or ALTER SEQUENCE statements. It processes a wide variety of sequence options including data type, increment, start/restart values, min/max bounds, cache size, and cycle behavior.

The function iterates through the provided options list, identifying and parsing each parameter while checking for conflicts (duplicate specifications). It performs extensive validation including type compatibility checks, range validation for different integer types (smallint, integer, bigint), and logical consistency checks between related parameters.

For ALTER SEQUENCE operations, the function determines whether a sequence rewrite is necessary based on which parameters are being changed. Most parameter changes require rewriting the sequence relation to maintain transactional behavior, with the notable exception of OWNED BY which must not cause a rewrite to preserve pg_upgrade compatibility.

The function also handles intelligent defaulting when changing sequence types, automatically adjusting min/max values when they were previously set to the old type's limits, and provides comprehensive cross-validation between interdependent parameters like start/restart values against min/max bounds.

## Parameters / Member Variables
- `pstate`: Parse state for error reporting and location tracking
- `options`: List of DefElem structures representing parsed sequence options
- `for_identity`: Boolean indicating if this is for an identity column (affects error messages)
- `isInit`: Boolean indicating initialization mode (vs. alteration mode)
- `seqform`: Form_pg_sequence structure to store catalog parameters
- `seqdataform`: Form_pg_sequence_data structure to store sequence data parameters
- `need_seq_rewrite`: Output parameter indicating if sequence relation rewrite is required
- `owned_by`: Output parameter containing OWNED BY specification or NIL

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_sequence (sequence catalog structure)
  - Form_pg_sequence_data (sequence data structure)
  - DefElem (parsed option element structure)
  - errorConflictingDefElem (reports conflicting option errors)
  - defGetQualifiedName (extracts qualified name from DefElem)
  - typenameTypeId (resolves type name to OID)
  - defGetTypeName (extracts type name from DefElem)
  - defGetInt64 (extracts 64-bit integer from DefElem)
  - boolVal (extracts boolean value)
  - BoolIsValid (validates boolean value)
  - Various type constants (INT2OID, INT4OID, INT8OID)
  - Various limit constants (PG_INT16_MIN/MAX, PG_INT32_MIN/MAX, PG_INT64_MIN/MAX)
- Called from (representative examples):
  - DefineSequence
  - AlterSequence

## Notes and Other Information
- Supports all standard sequence options: AS, INCREMENT, START, RESTART, MINVALUE, MAXVALUE, CACHE, CYCLE, OWNED BY
- Only supports smallint, integer, and bigint data types for sequences
- OWNED BY is the only option that doesn't trigger sequence rewrite (critical for pg_upgrade)
- Performs comprehensive validation including type range checking and cross-parameter consistency
- Handles intelligent defaulting when changing sequence data types
- The log_cnt field is reset whenever parameters affecting future nextval allocations are changed
- This is a static function internal to src/backend/commands/sequence.c