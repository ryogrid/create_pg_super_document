# getopt

## Location
src/port/getopt.c: 71 - 136

## Overview
Parse command-line arguments from an argc/argv vector, following POSIX getopt conventions.

## Definition
int getopt(int nargc, char *const *nargv, const char *ostr)

## Detailed Description
The  function is PostgreSQL's implementation of the standard POSIX getopt() utility, designed to parse command-line options and arguments. This implementation is used when the system's native getopt() is unavailable or when PostgreSQL requires specific behavior that differs from the system implementation.

Key features:
- Parses short options (single character preceded by '-')
- Handles options that require arguments
- Supports the '--' convention to terminate option parsing
- Maintains state between calls using static variables and global option variables
- Can be restarted on a new argv array by resetting optind to 1
- Does not use optreset, instead relying on internal state management

The function processes one option per call and maintains parsing state internally. It updates global variables (optind, optarg, optopt, opterr) to communicate results and state to the caller.

## Parameters / Member Variables
- : Number of arguments in the argv array (typically from main's argc)
- : Array of argument strings (typically from main's argv) 
- : Option string specifying valid options; ':' after a character indicates that option requires an argument

## Dependencies
- Functions called/Symbols referenced:
  - EMSG (empty string constant for resetting internal state)
  - BADCH (return value for invalid options, '?')
  - BADARG (return value for missing required argument, ':')
  - strchr (C library function to search for option character)
  - fprintf (C library function for error reporting)
- Called from (representative examples):
  - BootstrapModeMain
  - PostmasterMain
  - process_postgres_switches
  - main (in various test utilities and tools)

## Notes and Other Information
- This is a BSD-licensed implementation derived from the University of California's getopt
- Global variables used: optind (current argument index), optarg (argument for current option), optopt (current option character), opterr (error reporting flag)  
- Returns the option character when found, -1 when done, '?' for invalid options, and ':' for missing required arguments
- Handles the special case of '-' as a regular argument (not an option)
- The static variable 'place' maintains the current position within a multi-character option argument
- Error messages are printed to stderr when opterr is non-zero and the option string doesn't start with ':'
- Designed to be thread-unsafe due to static state, following traditional getopt() behavior