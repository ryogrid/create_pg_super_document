# pg_ls_replslotdir

## Location
[src/backend/utils/adt/genfile.c:715-733](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/genfile.c#L715-L733)

## Overview
Lists the files in a specific PostgreSQL replication slot directory within pg_replslot.

## Definition
```c
Datum pg_ls_replslotdir(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a SQL-callable PostgreSQL function that provides access to the contents of a specific replication slot directory within the pg_replslot directory. It takes a replication slot name as input, validates that the slot exists, and then returns detailed information about the regular files in that slot's directory. The function serves as a diagnostic and administrative tool for examining replication slot file contents.

The function performs validation to ensure the specified replication slot exists before attempting to list its directory contents, throwing an error if the slot is not found. This prevents unauthorized access to arbitrary directories and ensures the slot name corresponds to a valid replication slot.

## Parameters / Member Variables
- `fcinfo`: Function call information structure (standard PostgreSQL function parameter)
- `slotname_t`: Input text parameter containing the replication slot name
- `slotname`: C string version of the slot name
- `path`: Buffer to construct the full directory path

## Dependencies
- Functions called/Symbols referenced:
  - [text_to_cstring](../t/text_to_cstring.md)
  - [SearchNamedReplicationSlot](../S/SearchNamedReplicationSlot.md)
  - [pg_ls_dir_files](pg_ls_dir_files.md)
- Called from (representative examples):
  - Available as SQL function but no direct callers found in codebase

## Notes and Other Information
- This function requires a valid replication slot name as input parameter
- Validates slot existence using SearchNamedReplicationSlot before proceeding
- Throws ERRCODE_UNDEFINED_OBJECT error if the specified replication slot does not exist
- Constructs path as "pg_replslot/<slotname>" and lists regular files only
- Located in src/backend/utils/adt/genfile.c:715-733
- Part of PostgreSQL's replication slot management and diagnostic functionality
- Uses snprintf for safe path construction to prevent buffer overflows

## Simplified Source

```c
Datum
pg_ls_replslotdir(PG_FUNCTION_ARGS)
{
    text *slotname_t;
    char path[MAXPGPATH];
    char *slotname;

    // Extract replication slot name from function arguments
    slotname_t = PG_GETARG_TEXT_PP(0);
    slotname = text_to_cstring(slotname_t);

    // Validate that the replication slot exists
    if (!SearchNamedReplicationSlot(slotname, true))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("replication slot \"%s\" does not exist", slotname)));

    // Construct path to the replication slot directory
    snprintf(path, sizeof(path), "pg_replslot/%s", slotname);

    // List files in the replication slot directory
    return pg_ls_dir_files(fcinfo, path, false);
}
```