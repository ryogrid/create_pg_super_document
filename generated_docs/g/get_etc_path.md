# get_etc_path

## Location
src/port/path.c: 910 - 918

## Overview
Constructs the path to the PostgreSQL configuration directory (etc) relative to the PostgreSQL executable path.

## Definition


## Detailed Description
The  function calculates the absolute path to PostgreSQL's configuration directory by making a relative path calculation from the provided executable path. It uses the compile-time constants SYSCONFDIR and PGBINDIR to determine the proper relative location of the configuration directory. This function is essential for PostgreSQL components to locate configuration files like postgresql.conf when the installation location may vary from the compiled-in defaults.

## Parameters / Member Variables
- : The absolute path to the current PostgreSQL executable
- : Output buffer where the calculated configuration directory path will be stored

## Dependencies
- Functions called/Symbols referenced:
  - make_relative_path
- Called from (representative examples):
  - process_psqlrc (src/bin/psql/startup.c:785)
  - get_configdata (src/common/config_info.c:114)
  - set_pglocale_pgservice (src/common/exec.c:482)

## Notes and Other Information
- This function assumes that the caller has provided a sufficiently large buffer in ret_path to hold the resulting path
- The function relies on compile-time constants SYSCONFDIR and PGBINDIR which are set during the build process
- This is part of PostgreSQL's portable path resolution system that allows the software to work correctly even when moved from its original installation location