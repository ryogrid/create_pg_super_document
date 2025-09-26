# pro

## Location
[src/tools/pg_bsd_indent/args.c:86-175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/args.c#L86-L175)

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

```c
struct pro {
    const char *p_name;		/* name, e.g. -bl, -cli */
    int         p_type;		/* type (int, bool, special) */
    int         p_default;	/* the default value (if int) */
    int         p_special;	/* depends on type */
    int        *p_obj;		/* the associated variable */
}           pro[] = {

    {"T", PRO_SPECIAL, 0, KEY, 0},
    {"U", PRO_SPECIAL, 0, KEY_FILE, 0},
    {"-version", PRO_SPECIAL, 0, VERSION, 0},
    {"P", PRO_SPECIAL, 0, IGN, 0},
    {"bacc", PRO_BOOL, false, ON, &blanklines_around_conditional_compilation},
    {"badp", PRO_BOOL, false, ON, &blanklines_after_declarations_at_proctop},
    {"bad", PRO_BOOL, false, ON, &blanklines_after_declarations},
    {"bap", PRO_BOOL, false, ON, &blanklines_after_procs},
    {"bbb", PRO_BOOL, false, ON, &blanklines_before_blockcomments},
    {"bc", PRO_BOOL, true, OFF, &ps.leave_comma},
    {"bl", PRO_BOOL, true, OFF, &btype_2},
    {"br", PRO_BOOL, true, ON, &btype_2},
    {"bs", PRO_BOOL, false, ON, &Bill_Shannon},
    {"cdb", PRO_BOOL, true, ON, &comment_delimiter_on_blankline},
    {"cd", PRO_INT, 0, 0, &ps.decl_com_ind},
    {"ce", PRO_BOOL, true, ON, &cuddle_else},
    {"ci", PRO_INT, 0, 0, &continuation_indent},
    {"cli", PRO_SPECIAL, 0, CLI, 0},
    {"cp", PRO_INT, 0, 0, &else_endif_com_ind},
    {"c", PRO_INT, 33, 0, &ps.com_ind},
    {"di", PRO_INT, 16, 0, &ps.decl_indent},
    {"dj", PRO_BOOL, false, ON, &ps.ljust_decl},
    {"d", PRO_INT, 0, 0, &ps.unindent_displace},
    {"eei", PRO_BOOL, false, ON, &extra_expression_indent},
    {"ei", PRO_BOOL, true, ON, &ps.else_if},
    {"fbs", PRO_BOOL, true, ON, &function_brace_split},
    {"fc1", PRO_BOOL, true, ON, &format_col1_comments},
    {"fcb", PRO_BOOL, true, ON, &format_block_comments},
    {"ip", PRO_BOOL, true, ON, &ps.indent_parameters},
    {"i", PRO_INT, 8, 0, &ps.ind_size},
    {"lc", PRO_INT, 0, 0, &block_comment_max_col},
    {"ldi", PRO_INT, -1, 0, &ps.local_decl_indent},
    {"lpl", PRO_BOOL, false, ON, &lineup_to_parens_always},
    {"lp", PRO_BOOL, true, ON, &lineup_to_parens},
    {"l", PRO_INT, 78, 0, &max_col},
    {"nbacc", PRO_BOOL, false, OFF, &blanklines_around_conditional_compilation},
    {"nbadp", PRO_BOOL, false, OFF, &blanklines_after_declarations_at_proctop},
    {"nbad", PRO_BOOL, false, OFF, &blanklines_after_declarations},
    {"nbap", PRO_BOOL, false, OFF, &blanklines_after_procs},
    {"nbbb", PRO_BOOL, false, OFF, &blanklines_before_blockcomments},
    {"nbc", PRO_BOOL, true, ON, &ps.leave_comma},
    {"nbs", PRO_BOOL, false, OFF, &Bill_Shannon},
    {"ncdb", PRO_BOOL, true, OFF, &comment_delimiter_on_blankline},
    {"nce", PRO_BOOL, true, OFF, &cuddle_else},
    {"ndj", PRO_BOOL, false, OFF, &ps.ljust_decl},
    {"neei", PRO_BOOL, false, OFF, &extra_expression_indent},
    {"nei", PRO_BOOL, true, OFF, &ps.else_if},
    {"nfbs", PRO_BOOL, true, OFF, &function_brace_split},
    {"nfc1", PRO_BOOL, true, OFF, &format_col1_comments},
    {"nfcb", PRO_BOOL, true, OFF, &format_block_comments},
    {"nip", PRO_BOOL, true, OFF, &ps.indent_parameters},
    {"nlpl", PRO_BOOL, false, OFF, &lineup_to_parens_always},
    {"nlp", PRO_BOOL, true, OFF, &lineup_to_parens},
    {"npcs", PRO_BOOL, false, OFF, &proc_calls_space},
    {"npro", PRO_SPECIAL, 0, IGN, 0},
    {"npsl", PRO_BOOL, true, OFF, &procnames_start_line},
    {"nsac", PRO_BOOL, false, OFF, &space_after_cast},
    {"nsc", PRO_BOOL, true, OFF, &star_comment_cont},
    {"nsob", PRO_BOOL, false, OFF, &swallow_optional_blanklines},
    {"ntpg", PRO_BOOL, false, OFF, &postgres_tab_rules},
    {"nut", PRO_BOOL, true, OFF, &use_tabs},
    {"nv", PRO_BOOL, false, OFF, &verbose},
    {"pcs", PRO_BOOL, false, ON, &proc_calls_space},
    {"psl", PRO_BOOL, true, ON, &procnames_start_line},
    {"sac", PRO_BOOL, false, ON, &space_after_cast},
    {"sc", PRO_BOOL, true, ON, &star_comment_cont},
    {"sob", PRO_BOOL, false, ON, &swallow_optional_blanklines},
    {"st", PRO_SPECIAL, 0, STDIN, 0},
    {"ta", PRO_BOOL, false, ON, &auto_typedefs},
    {"tpg", PRO_BOOL, false, ON, &postgres_tab_rules},
    {"ts", PRO_INT, 8, 0, &tabsize},
    {"ut", PRO_BOOL, true, ON, &use_tabs},
    {"v", PRO_BOOL, false, ON, &verbose},
    /* whew! */
    {0, 0, 0, 0, 0}
};
```
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
  - [set_defaults](../s/set_defaults.md) (src/tools/pg_bsd_indent/args.c:248, 255)
  - [set_option](../s/set_option.md) (src/tools/pg_bsd_indent/args.c:263, 267)

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