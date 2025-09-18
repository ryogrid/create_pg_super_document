# read_controlfile

## Location
[src/bin/pg_resetwal/pg_resetwal.c:559-632](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_resetwal/pg_resetwal.c#L559-L632)

## Overview
read_controlfile attempts to read and validate the existing pg_control file, performing CRC checks and version validation to determine if the control file data can be trusted.

## Definition


## Detailed Description
This function is responsible for reading the PostgreSQL control file (pg_control) and determining whether its contents are valid and usable. The function performs several levels of validation:

1. **File Access**: Opens the pg_control file for reading, providing helpful error messages if the file doesn't exist
2. **Size and Version Check**: Verifies that the file is large enough and contains the expected control file version
3. **CRC Validation**: Computes and verifies the CRC32C checksum to detect corruption
4. **WAL Segment Size Validation**: Ensures the WAL segment size specified in the control file is valid

The function uses a global  flag to indicate when data might be unreliable (e.g., when CRC validation fails). It returns true if the control file appears to be valid and usable, false if it's corrupted or has an invalid WAL segment size.

If the control file cannot be read at all, the function provides helpful hints (like using 'touch pg_control' to create an empty file) before terminating the program.

## Parameters / Member Variables
This function takes no parameters and returns a boolean indicating success/validity.

## Dependencies
- Functions called/Symbols referenced:
  - open (file opening with O_RDONLY and PG_BINARY flags)
  - pg_malloc (memory allocation)
  - read (file reading)
  - close (file closing)
  - pg_log_error (error logging)
  - pg_log_error_hint (hint logging)
  - pg_log_warning (warning logging)
  - [pg_fatal](../p/pg_fatal.md) (fatal error handling)
  - memcpy (memory copying)
  - IsValidWalSegSize (WAL segment size validation)
  - ngettext (internationalized messaging)
  - INIT_CRC32C, COMP_CRC32C, FIN_CRC32C, EQ_CRC32C (CRC calculation macros)
- Called from:
  - [main](../m/main.md) (in pg_resetwal.c:383)

## Notes and Other Information
- This is a static function, accessible only within pg_resetwal.c
- The function handles multiple error conditions gracefully, providing specific guidance for common issues
- Uses maxaligned buffer allocation to ensure proper memory alignment for the control file data
- Sets the global  variable when CRC validation fails but data is otherwise usable
- The function is designed to be fault-tolerant - it can work with corrupted control files by marking data as 'guessed'
- Currently includes placeholder code for updating old pg_control versions, though no actual version migration is implemented
- Returns false for invalid WAL segment sizes, which would prevent pg_resetwal from proceeding with potentially dangerous operations