# pgwin32_install_crashdump_handler

## Location
src/backend/port/win32/crashdump.c: 178 - 181

## Overview
A Windows-specific initialization function that installs PostgreSQL's crash dump handler to capture debugging information when unhandled exceptions occur.

## Definition


## Detailed Description
This function serves as the entry point for enabling PostgreSQL's crash dump functionality on Windows systems. It registers the crashDumpHandler function as the system's unhandled exception filter using the Windows API SetUnhandledExceptionFilter. Once installed, any unhandled exception that would normally terminate the process will first be processed by PostgreSQL's custom crash dump handler.

The function is typically called during PostgreSQL startup to ensure crash dump generation is available throughout the server's lifetime. This provides valuable debugging information when PostgreSQL encounters fatal errors or crashes, helping developers and administrators diagnose issues in production environments.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - SetUnhandledExceptionFilter (Windows API)
  - [crashDumpHandler](../c/crashDumpHandler.md)

- Called from (representative examples):
  - [main](../m/main.md) (from src/backend/main/main.c)

## Notes and Other Information
- Windows-specific functionality, only available on Windows builds of PostgreSQL
- Must be called early in PostgreSQL startup to ensure crash protection coverage
- The installed handler remains active for the lifetime of the process
- Part of PostgreSQL's Windows port infrastructure for improved debugging support
- Works in conjunction with the crashDumpHandler to provide comprehensive crash dump generation
- Should only be called once during process initialization