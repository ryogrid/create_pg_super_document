# pro

## Location
src/tools/pg_bsd_indent/args.c: 86 - 175

## Overview
The usage: pro [-h] [--debug] [--version] <command> ...

Quick start commands:

  status           current status of all Ubuntu Pro services
  attach           attach this machine to an Ubuntu Pro subscription
  enable           enable a specific Ubuntu Pro service on this machine
  system           show system information related to Pro services
  security-status  list available security updates for the system

Security-related commands:

  fix              check for and mitigate the impact of a CVE/USN on this system

Troubleshooting-related commands:

  collect-logs     collect Pro logs and debug information

Other commands:

  api              Calls the Client API endpoints.
  auto-attach      automatically attach on supported platforms
  config           manage Ubuntu Pro configuration on this machine
  detach           remove this machine from an Ubuntu Pro subscription
  disable          disable a specific Ubuntu Pro service on this machine
  refresh          refresh Ubuntu Pro services

Flags:

  -h, --help       Displays help on pro and command line options
  --debug          show all debug log messages to console
  --version        show version of pro

Use pro <command> --help for more information about a command. symbol is a static configuration table that defines all command-line options supported by PostgreSQL's BSD-style code formatter (pg_bsd_indent), mapping option names to their types, default values, and associated variables.

## Definition


## Detailed Description
The usage: pro [-h] [--debug] [--version] <command> ...

Quick start commands:

  status           current status of all Ubuntu Pro services
  attach           attach this machine to an Ubuntu Pro subscription
  enable           enable a specific Ubuntu Pro service on this machine
  system           show system information related to Pro services
  security-status  list available security updates for the system

Security-related commands:

  fix              check for and mitigate the impact of a CVE/USN on this system

Troubleshooting-related commands:

  collect-logs     collect Pro logs and debug information

Other commands:

  api              Calls the Client API endpoints.
  auto-attach      automatically attach on supported platforms
  config           manage Ubuntu Pro configuration on this machine
  detach           remove this machine from an Ubuntu Pro subscription
  disable          disable a specific Ubuntu Pro service on this machine
  refresh          refresh Ubuntu Pro services

Flags:

  -h, --help       Displays help on pro and command line options
  --debug          show all debug log messages to console
  --version        show version of pro

Use pro <command> --help for more information about a command. table serves as the central configuration registry for pg_bsd_indent's command-line options. Each entry in the array describes one formatting option, including its name (such as "-bl" for brace formatting or "-i" for indentation size), its data type (boolean, integer, or special), default values, and a pointer to the global variable that stores the option's current value.

The table is carefully ordered so that options whose names are substrings of other options appear later in the array. This ordering is critical because the option parsing logic scans the table sequentially and matches the first option name that fits. For example, "-lp" must appear before "-l" to ensure proper parsing.

The table supports three main categories of options:
- **PRO_BOOL**: Boolean flags that can be turned on/off (e.g., brace formatting styles, spacing options)
- **PRO_INT**: Integer parameters that control numeric settings (e.g., indentation size, column limits)  
- **PRO_SPECIAL**: Special-purpose options that require custom handling (e.g., profile loading, version display)

## Parameters / Member Variables
- : String containing the option name as it appears on the command line (e.g., "bl", "i", "cli")
- : Option type constant - PRO_BOOL (2), PRO_INT (3), or PRO_SPECIAL (1)
- : Default value for integer options, or boolean state for boolean options
- : Additional type-specific information, often used for ON/OFF constants or special operation codes
- : Pointer to the global variable that stores this option's runtime value

## Dependencies
- Functions called/Symbols referenced:
  - PRO_BOOL, PRO_INT, PRO_SPECIAL (type constants)
  - Various global formatting variables (e.g., blanklines_around_conditional_compilation, ps.ind_size)
- Called from (representative examples):
  - set_defaults (src/tools/pg_bsd_indent/args.c:248, 255)
  - set_option (src/tools/pg_bsd_indent/args.c:263, 267)

## Notes and Other Information
The usage: pro [-h] [--debug] [--version] <command> ...

Quick start commands:

  status           current status of all Ubuntu Pro services
  attach           attach this machine to an Ubuntu Pro subscription
  enable           enable a specific Ubuntu Pro service on this machine
  system           show system information related to Pro services
  security-status  list available security updates for the system

Security-related commands:

  fix              check for and mitigate the impact of a CVE/USN on this system

Troubleshooting-related commands:

  collect-logs     collect Pro logs and debug information

Other commands:

  api              Calls the Client API endpoints.
  auto-attach      automatically attach on supported platforms
  config           manage Ubuntu Pro configuration on this machine
  detach           remove this machine from an Ubuntu Pro subscription
  disable          disable a specific Ubuntu Pro service on this machine
  refresh          refresh Ubuntu Pro services

Flags:

  -h, --help       Displays help on pro and command line options
  --debug          show all debug log messages to console
  --version        show version of pro

Use pro <command> --help for more information about a command. table contains over 70 different formatting options, making pg_bsd_indent highly configurable for different coding styles. Many options have both positive and negative forms (e.g., "bacc" and "nbacc") to allow explicit enabling or disabling. The table is terminated with a NULL entry  to mark the end of the options list.

The ordering constraint mentioned in the code comments is crucial for correct operation - substring options must appear after their parent strings to prevent premature matches during option parsing.