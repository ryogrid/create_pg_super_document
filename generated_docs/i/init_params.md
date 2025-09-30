# init_params

## Location
[src/backend/commands/sequence.c:1257-1592](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/sequence.c#L1257-L1592)

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
  - [DefElem](../D/DefElem.md) (parsed option element structure)
  - [errorConflictingDefElem](../e/errorConflictingDefElem.md) (reports conflicting option errors)
  - [defGetQualifiedName](../d/defGetQualifiedName.md) (extracts qualified name from DefElem)
  - [typenameTypeId](../t/typenameTypeId.md) (resolves type name to OID)
  - [defGetTypeName](../d/defGetTypeName.md) (extracts type name from DefElem)
  - [defGetInt64](../d/defGetInt64.md) (extracts 64-bit integer from DefElem)
  - boolVal (extracts boolean value)
  - BoolIsValid (validates boolean value)
  - Various type constants (INT2OID, INT4OID, INT8OID)
  - Various limit constants (PG_INT16_MIN/MAX, PG_INT32_MIN/MAX, PG_INT64_MIN/MAX)
- Called from (representative examples):
  - [DefineSequence](../D/DefineSequence.md)
  - [AlterSequence](../A/AlterSequence.md)

## Notes and Other Information
- Supports all standard sequence options: AS, INCREMENT, START, RESTART, MINVALUE, MAXVALUE, CACHE, CYCLE, OWNED BY
- Only supports smallint, integer, and bigint data types for sequences
- OWNED BY is the only option that doesn't trigger sequence rewrite (critical for pg_upgrade)
- Performs comprehensive validation including type range checking and cross-parameter consistency
- Handles intelligent defaulting when changing sequence data types
- The log_cnt field is reset whenever parameters affecting future nextval allocations are changed
- This is a static function internal to src/backend/commands/sequence.c

## Simplified Source

```c
static void
init_params(ParseState *pstate, List *options, bool for_identity,
            bool isInit,
            Form_pg_sequence seqform,
            Form_pg_sequence_data seqdataform,
            bool *need_seq_rewrite,
            List **owned_by)
{
    // Initialize option holders
    DefElem *as_type = NULL, *start_value = NULL, *restart_value = NULL;
    DefElem *increment_by = NULL, *max_value = NULL, *min_value = NULL;
    DefElem *cache_value = NULL, *is_cycled = NULL;
    bool reset_max_value = false, reset_min_value = false;

    *need_seq_rewrite = false;
    *owned_by = NIL;

    // Parse each option from the list
    foreach(option, options) {
        DefElem *defel = (DefElem *) lfirst(option);

        // Handle each option type (as, increment, start, restart, etc.)
        if (strcmp(defel->defname, "as") == 0) {
            if (as_type) errorConflictingDefElem(defel, pstate);
            as_type = defel;
            *need_seq_rewrite = true;
        }
        // ... similar handling for other options ...
        else if (strcmp(defel->defname, "owned_by") == 0) {
            *owned_by = defGetQualifiedName(defel);
        }
    }

    // Reset log counter for initialization or parameter changes
    if (isInit) seqdataform->log_cnt = 0;

    // Process AS (data type) option
    if (as_type != NULL) {
        Oid newtypid = typenameTypeId(pstate, defGetTypeName(as_type));

        // Validate type is smallint, int, or bigint
        if (newtypid != INT2OID && newtypid != INT4OID && newtypid != INT8OID)
            ereport(ERROR, "sequence type must be smallint, integer, or bigint");

        // Handle type conversion for existing sequences
        if (!isInit) {
            // Reset min/max if they were at old type limits
            if (seqform->seqmax == old_type_max) reset_max_value = true;
            if (seqform->seqmin == old_type_min) reset_min_value = true;
        }
        seqform->seqtypid = newtypid;
    }
    else if (isInit) {
        seqform->seqtypid = INT8OID;  // Default to bigint
    }

    // Process INCREMENT BY option
    if (increment_by != NULL) {
        seqform->seqincrement = defGetInt64(increment_by);
        if (seqform->seqincrement == 0)
            ereport(ERROR, "INCREMENT must not be zero");
        seqdataform->log_cnt = 0;
    }
    else if (isInit) {
        seqform->seqincrement = 1;
    }

    // Process MAXVALUE and MINVALUE with defaults and validation
    // Set appropriate defaults based on sequence direction and type
    if (max_value processing or defaults needed) {
        if (seqform->seqincrement > 0) {
            // Ascending sequence - use type maximum
            seqform->seqmax = get_type_maximum(seqform->seqtypid);
        } else {
            seqform->seqmax = -1;  // Descending sequence
        }
    }

    // Validate min/max are within type bounds and consistent
    validate_sequence_bounds(seqform);

    // Process START WITH and RESTART options
    if (start_value != NULL) {
        seqform->seqstart = defGetInt64(start_value);
    }
    else if (isInit) {
        // Default start: min for ascending, max for descending
        seqform->seqstart = (seqform->seqincrement > 0) ?
            seqform->seqmin : seqform->seqmax;
    }

    // Validate start/restart values are within bounds
    validate_start_restart_values(seqform, seqdataform);

    // Process CACHE option
    if (cache_value != NULL) {
        seqform->seqcache = defGetInt64(cache_value);
        if (seqform->seqcache <= 0)
            ereport(ERROR, "CACHE must be greater than zero");
        seqdataform->log_cnt = 0;
    }
    else if (isInit) {
        seqform->seqcache = 1;
    }
}
```