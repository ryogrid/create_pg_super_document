# set_starttype

## Location
src/bin/pg_ctl/pg_ctl.c: 2092 - 2112

## Overview
A static function in pg_ctl that parses and sets the Windows service start type for PostgreSQL server registration, mapping string options to Windows service constants.

## Definition


## Detailed Description
The  function processes start type options for Windows service registration functionality in pg_ctl. It accepts both short and long forms of start type specifications and configures the global  variable accordingly. This function is specifically used when registering PostgreSQL as a Windows service to determine how the service should be started.

The function supports two start types:
- **Auto start** ("a" or "auto"): Sets  - service starts automatically during system boot
- **Demand start** ("d" or "demand"): Sets  - service starts only when explicitly requested

The function validates the input and terminates the program with an error message if an invalid start type is provided.

## Parameters / Member Variables
- : String containing the start type specification (short or long form: "a"/"auto" or "d"/"demand")

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (string comparison)
  - SERVICE_AUTO_START, SERVICE_DEMAND_START (Windows service constants)
  - [write_stderr](../w/write_stderr.md) (error output function)
  - [do_advice](../d/do_advice.md) (help/advice function)
  - exit (program termination)
  - pgctl_start_type (global variable)

- Called from (representative examples):
  - [main](../m/main.md) (when processing -S start-type option during service registration)

## Notes and Other Information
- This function is Windows-specific and relates to the service registration functionality of pg_ctl
- The function modifies the global  variable used during Windows service registration
- Both abbreviated (a/d) and full (auto/demand) start type names are supported
- Error handling terminates the program immediately on invalid start types
- The "auto" start type is typically the default for production PostgreSQL installations
- The "demand" start type is useful for development or specialized deployment scenarios
- Error messages are internationalized using the  macro
- Located in src/bin/pg_ctl/pg_ctl.c:2092-2112