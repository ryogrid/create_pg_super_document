# do_lo_export

## Location
src/bin/psql/large_obj.c: 142 - 175

## Overview
Exports a PostgreSQL large object to a file on the local filesystem, handling the complete workflow including transaction management, error handling, and user feedback.

## Definition
```c
bool do_lo_export(const char *loid_arg, const char *filename_arg)
```

## Detailed Description
This function implements the \\lo_export psql command functionality, which exports a large object from the PostgreSQL database to a local file. It manages the complete operation lifecycle including transaction setup, cancellation handling, the actual export operation using libpq's lo_export function, proper cleanup, and user feedback. The function ensures that the export operation occurs within a transaction context and handles both success and failure scenarios appropriately. It includes proper cancellation support to allow users to interrupt long-running exports.

## Parameters / Member Variables
- `loid_arg`: String representation of the large object OID to export
- `filename_arg`: Target filename/path where the large object content will be written

## Dependencies
- Functions called/Symbols referenced:
  - start_lo_xact (initialize transaction for large object operation)
  - atooid (convert string OID to numeric OID)
  - lo_export (libpq function to export large object to file)
  - SetCancelConn/ResetCancelConn (cancellation handling utilities)
  - fail_lo_xact (cleanup after failure)
  - finish_lo_xact (cleanup after success)
  - print_lo_result (output success message)
  - pg_log_info (error logging)
  - PQerrorMessage (get detailed error message from libpq)
- Called from (representative examples):
  - exec_command_lo (psql command dispatcher)

## Notes and Other Information
- Returns true on success, false on failure
- Non-static function, part of the public interface of large_obj.c
- Includes comment noting that lo_export return status documentation is lacking
- Uses proper cancellation handling to support user interruption
- Part of psql's large object management commands (\\lo_export, \\lo_import, \\lo_unlink)
- Implements complete error handling with transaction rollback on failure
- Success status from lo_export is checked as 1, with any other value indicating failure