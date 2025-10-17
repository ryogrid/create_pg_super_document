# _doSetFixedOutputState

## Location
[src/bin/pg_dump/pg_backup_archiver.c:3255-3321](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L3255-L3321)

## Overview
A static function that establishes consistent PostgreSQL session state by issuing SET commands for parameters that should remain constant throughout the execution of a restore script.

## Definition
```c
static void _doSetFixedOutputState(ArchiveHandle *AH)
```

## Detailed Description
This function configures the PostgreSQL session environment to ensure consistent behavior during database restoration operations. It sets various session parameters to predetermined values that are optimal for restore operations, including timeouts, encoding settings, search paths, and behavioral flags. The function is crucial for ensuring that restore operations behave predictably regardless of the server's default configuration or current session state.

Key configuration areas include:
- Disabling various timeouts to prevent interruption of long-running restore operations
- Setting character encoding and string literal syntax to match the dump
- Configuring the active role and search path
- Disabling function body checking and setting XML processing options
- Managing row-level security settings
- Initiating transactions when using transaction-size mode

## Parameters / Member Variables
- `AH`: Pointer to ArchiveHandle structure containing restore context and options

## Dependencies
- Functions called/Symbols referenced:
  - [RestoreOptions](../R/RestoreOptions.md) (struct type)
  - [ahprintf](../a/ahprintf.md) (archive output function)
  - [pg_encoding_to_char](../p/pg_encoding_to_char.md) (encoding conversion function)
  - [fmtId](../f/fmtId.md) (identifier formatting function) 
  - [StartTransaction](../S/StartTransaction.md) (transaction management function)
- Called from (representative examples):
  - [RestoreArchive](../R/RestoreArchive.md)
  - [_reconnectToDB](../r/_reconnectToDB.md)
  - [restore_toc_entries_postfork](../r/restore_toc_entries_postfork.md)
  - [CloneArchive](../C/CloneArchive.md)

## Notes and Other Information
- Located in src/bin/pg_dump/pg_backup_archiver.c:3255-3321
- Essential for ensuring restore operations work consistently across different PostgreSQL configurations
- Handles both connected and non-connected restore modes appropriately
- In transaction-size mode, initiates the first transaction and resets the transaction counter
- Sets client_min_messages to 'warning' to reduce noise during restore
- Conditionally disables escape_string_warning for non-standard-conforming string modes

## Simplified Source

```c
static void _doSetFixedOutputState(ArchiveHandle *AH) {
    RestoreOptions *ropt = AH->public.ropt;

    // Disable timeouts for long-running operations
    ahprintf(AH, "SET statement_timeout = 0;\n");
    ahprintf(AH, "SET lock_timeout = 0;\n");
    ahprintf(AH, "SET idle_in_transaction_session_timeout = 0;\n");
    ahprintf(AH, "SET transaction_timeout = 0;\n");

    // Set encoding and string handling
    ahprintf(AH, "SET client_encoding = '%s';\n",
             pg_encoding_to_char(AH->public.encoding));
    ahprintf(AH, "SET standard_conforming_strings = %s;\n",
             AH->public.std_strings ? "on" : "off");

    // Set role and search path if specified
    if (ropt && ropt->use_role)
        ahprintf(AH, "SET ROLE %s;\n", fmtId(ropt->use_role));
    if (AH->public.searchpath)
        ahprintf(AH, "%s", AH->public.searchpath);

    // Configure restore-friendly settings
    ahprintf(AH, "SET check_function_bodies = false;\n");
    ahprintf(AH, "SET xmloption = content;\n");
    ahprintf(AH, "SET client_min_messages = warning;\n");

    // Handle row security
    if (ropt && ropt->enable_row_security)
        ahprintf(AH, "SET row_security = on;\n");
    else
        ahprintf(AH, "SET row_security = off;\n");

    // Start transaction if using transaction-size mode
    if (ropt && ropt->txn_size > 0) {
        if (AH->connection)
            StartTransaction(&AH->public);
        else
            ahprintf(AH, "\nBEGIN;\n");
        AH->txnCount = 0;
    }

    ahprintf(AH, "\n");
}
```