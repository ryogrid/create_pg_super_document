# default_identify

## Location
src/bin/pg_waldump/rmgrdesc.c: 62 - 72

## Overview
Provides a default identification function for custom resource manager records that returns NULL to indicate no specific identification is available.

## Definition
```c
static const char *default_identify(uint8 info)
```

## Detailed Description
The `default_identify` function serves as a fallback identification function for custom resource manager WAL records in pg_waldump. Since custom resource managers' record formats and info codes are unknown to pg_waldump, this function simply returns NULL to indicate that no specific identification string can be provided for the record type. The caller is expected to handle the NULL return value appropriately.

## Parameters / Member Variables
- `info`: uint8 value containing the info code from the WAL record that would normally be used to identify the specific record type within a resource manager

## Dependencies
- Functions called/Symbols referenced:
  - None (simply returns NULL)
- Called from:
  - [initialize_custom_rmgrs](../i/initialize_custom_rmgrs.md) (assigned as the identification function for custom resource managers)

## Notes and Other Information
- This function is static and only used within rmgrdesc.c
- Always returns NULL since custom resource managers' record type information is not available to pg_waldump
- Used as a fallback when custom resource managers don't provide their own identification functions
- Part of the pg_waldump utility for analyzing WAL (Write-Ahead Logging) files
- The info parameter is accepted but ignored since there's no way to interpret custom resource manager info codes