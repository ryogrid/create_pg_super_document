# config_sspi_auth

## Location
[src/test/regress/pg_regress.c:999-1053](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L999-L1053)

## Overview
Configures SSPI authentication for PostgreSQL regression tests by rewriting pg_hba.conf and pg_ident.conf to permit the current Windows OS user to authenticate as the bootstrap superuser and any user specified via --create-role option.

## Definition

```c
struct addrinfo *gai_result;
```
## Detailed Description
This function is specifically designed for Windows environments to set up SSPI (Security Support Provider Interface) authentication during PostgreSQL regression testing. It creates new pg_hba.conf and pg_ident.conf files that allow the current Windows user to authenticate as the database superuser and any additional roles specified during test setup. The function handles both IPv4 and IPv6 connections, automatically detecting IPv6 support on the platform.

The function writes configuration entries that map the current Windows domain user to PostgreSQL database users using SSPI authentication with realm inclusion and a regress identity map. This enables seamless authentication during regression tests without requiring password-based authentication.

## Parameters / Member Variables
- : Directory path where PostgreSQL data files are stored, used to locate and write the configuration files
- : Name of the bootstrap superuser; if NULL, the function determines the default superuser name using the same method as initdb

## Dependencies
- Functions called/Symbols referenced:
  - [current_windows_user](current_windows_user.md)
  - [get_user_name](../g/get_user_name.md)
  - bail
  - fopen/fclose/fputs/fprintf (standard I/O)
  - WSAStartup/getaddrinfo (Windows networking)
  - [fmtHba](../f/fmtHba.md)
- Called from (representative examples):
  - [regression_main](../r/regression_main.md) (in --config-auth mode)

## Notes and Other Information
- This function is only available when compiled with ENABLE_SSPI support
- The function creates a CW macro for error checking write operations
- IPv6 support detection follows the same logic as initdb.c:setup_config()
- The generated pg_ident.conf includes entries for both the superuser and any extra roles specified via extraroles
- Account names are double-quoted to handle whitespace and '#' characters
- The function will terminate the program (via bail()) if critical operations fail, such as file creation or writing