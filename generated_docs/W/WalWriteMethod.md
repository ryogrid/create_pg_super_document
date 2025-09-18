# WalWriteMethod

## Location
src/bin/pg_basebackup/walmethods.h: 15 - 28

## Overview
WalWriteMethod is a structure that represents a way of writing streaming WAL (Write-Ahead Log) data as it's received in PostgreSQL backup utilities.

## Definition


## Detailed Description
WalWriteMethod is an abstract base structure used in PostgreSQL's backup utilities (pg_basebackup and pg_receivewal) to provide a polymorphic interface for writing WAL data to different destinations. The structure uses a vtable pattern through the WalWriteMethodOps function pointer table to support different storage methods like writing to regular files in a directory or to tar archives.

This design allows the backup utilities to write WAL data without knowing the specific implementation details of how the data is stored. All methods that have a failure return indicator will set either lasterrstring or lasterrno (with lasterrstring taking precedence) so that callers can signal appropriate errors.

The structure is designed to be embedded as the first member of larger, method-specific structures that contain additional fields relevant to each particular implementation.

## Parameters / Member Variables
- : Pointer to a WalWriteMethodOps structure containing function pointers for all operations supported by this method
- : The compression algorithm to use (from pg_compress_algorithm enum)
- : Compression level setting for the chosen algorithm
- : Boolean flag indicating whether to perform fsync operations for data durability
- : String description of the last error (takes precedence over lasterrno if set)
- : Numeric error code of the last error (used when lasterrstring is NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [WalWriteMethodOps](WalWriteMethodOps.md)
  - [pg_compress_algorithm](../p/pg_compress_algorithm.md)
  - pgoff_t

- Called from (representative examples):
  - [CreateWalDirectoryMethod](../C/CreateWalDirectoryMethod.md) (src/bin/pg_basebackup/walmethods.c:640)
  - [CreateWalTarMethod](../C/CreateWalTarMethod.md) (src/bin/pg_basebackup/walmethods.c:1364)
  - [StreamCtl](../S/StreamCtl.md) structure (src/bin/pg_basebackup/receivelog.h:46)
  - [GetLastWalMethodError](../G/GetLastWalMethodError.md) (src/bin/pg_basebackup/walmethods.c:1383)

## Notes and Other Information
- Two concrete implementations are available: WalDirectoryMethod (for writing to regular files) and WalTarMethod (for writing to tar archives)
- The structure uses inheritance-like behavior in C by being embedded as the first member of implementation-specific structures
- Error handling follows a pattern where either lasterrstring or lasterrno is set, with the string taking precedence
- This abstraction enables pg_basebackup to support multiple output formats without code duplication
- The sync flag controls whether data is immediately fsynced to disk for durability guarantees