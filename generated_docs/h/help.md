# help

## Location
[src/bin/scripts/vacuumdb.c:1168-1212](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/scripts/vacuumdb.c#L1168-L1212)

## Overview
This function displays comprehensive command-line help information for the PostgreSQL server, including all available options for different operational modes and usage scenarios.

## Definition

```c
static void
help(const char *progname)
```
## Detailed Description
The GNU bash, version 5.1.16(1)-release (x86_64-pc-linux-gnu)
These shell commands are defined internally.  Type `help' to see this list.
Type `help name' to find out more about the function `name'.
Use `info bash' to find out more about the shell in general.
Use `man -k' or `info' to find out more about commands not in this list.

A star (*) next to a name means that the command is disabled.

 job_spec [&]                            history [-c] [-d offset] [n] or hist>
 (( expression ))                        if COMMANDS; then COMMANDS; [ elif C>
 . filename [arguments]                  jobs [-lnprs] [jobspec ...] or jobs >
 :                                       kill [-s sigspec | -n signum | -sigs>
 [ arg... ]                              let arg [arg ...]
 [[ expression ]]                        local [option] name[=value] ...
 alias [-p] [name[=value] ... ]          logout [n]
 bg [job_spec ...]                       mapfile [-d delim] [-n count] [-O or>
 bind [-lpsvPSVX] [-m keymap] [-f file>  popd [-n] [+N | -N]
 break [n]                               printf [-v var] format [arguments]
 builtin [shell-builtin [arg ...]]       pushd [-n] [+N | -N | dir]
 caller [expr]                           pwd [-LP]
 case WORD in [PATTERN [| PATTERN]...)>  read [-ers] [-a array] [-d delim] [->
 cd [-L|[-P [-e]] [-@]] [dir]            readarray [-d delim] [-n count] [-O >
 command [-pVv] command [arg ...]        readonly [-aAf] [name[=value] ...] o>
 compgen [-abcdefgjksuv] [-o option] [>  return [n]
 complete [-abcdefgjksuv] [-pr] [-DEI]>  select NAME [in WORDS ... ;] do COMM>
 compopt [-o|+o option] [-DEI] [name .>  set [-abefhkmnptuvxBCHP] [-o option->
 continue [n]                            shift [n]
 coproc [NAME] command [redirections]    shopt [-pqsu] [-o] [optname ...]
 declare [-aAfFgiIlnrtux] [-p] [name[=>  source filename [arguments]
 dirs [-clpv] [+N] [-N]                  suspend [-f]
 disown [-h] [-ar] [jobspec ... | pid >  test [expr]
 echo [-neE] [arg ...]                   time [-p] pipeline
 enable [-a] [-dnps] [-f filename] [na>  times
 eval [arg ...]                          trap [-lp] [[arg] signal_spec ...]
 exec [-cl] [-a name] [command [argume>  true
 exit [n]                                type [-afptP] name [name ...]
 export [-fn] [name[=value] ...] or ex>  typeset [-aAfFgiIlnrtux] [-p] name[=>
 false                                   ulimit [-SHabcdefiklmnpqrstuvxPT] [l>
 fc [-e ename] [-lnr] [first] [last] o>  umask [-p] [-S] [mode]
 fg [job_spec]                           unalias [-a] name [name ...]
 for NAME [in WORDS ... ] ; do COMMAND>  unset [-f] [-v] [-n] [name ...]
 for (( exp1; exp2; exp3 )); do COMMAN>  until COMMANDS; do COMMANDS; done
 function name { COMMANDS ; } or name >  variables - Names and meanings of so>
 getopts optstring name [arg ...]        wait [-fn] [-p var] [id ...]
 hash [-lr] [-p pathname] [-dt] [name >  while COMMANDS; do COMMANDS; done
 help [-dms] [pattern ...]               { COMMANDS ; } function provides a complete reference of command-line options available for the PostgreSQL server executable. It outputs formatted help text that covers multiple operational modes:

1. **Standard server options**: Core PostgreSQL server configuration options
2. **Developer options**: Advanced options primarily used for debugging and development
3. **Single-user mode options**: Options specific to single-user database operation
4. **Bootstrap mode options**: Options for database initialization and checking

The function uses internationalization support through the  macro for translatable messages, ensuring help text can be localized. It also conditionally displays SSL-related options based on compile-time configuration.

## Parameters / Member Variables
- `*progname`: String containing the program name (typically "postgres") used in help text formatting
## Dependencies
- Functions called/Symbols referenced:
  - printf (standard C library function for formatted output)
  -  macro (PostgreSQL internationalization macro)
  - USE_SSL (conditional compilation macro)
  - PACKAGE_BUGREPORT (build-time constant for bug reporting address)
  - PACKAGE_NAME (build-time constant for package name)
  - PACKAGE_URL (build-time constant for package URL)
- Called from:
  - [main](../m/main.md) (in various PostgreSQL utilities and the main server)
  - Multiple PostgreSQL command-line utilities for help display

## Notes and Other Information
- The function is static within main.c but the pattern is replicated across many PostgreSQL utilities
- Output formatting matches options accepted by PostmasterMain() and PostgresMain()
- Includes a note about Windows console display limitations for non-ASCII characters
- Displays different option categories with clear section headers
- SSL options are conditionally compiled based on USE_SSL preprocessor definition
- Provides comprehensive coverage including debugging levels, connection parameters, memory settings, and operational modes
- Help text includes references to documentation and bug reporting information
- Function design ensures consistency across PostgreSQL command-line tools

## Simplified Source
```c
static void help(const char *progname) {
    // Basic program introduction
    printf(_("%s is the PostgreSQL server.\n\n"), progname);
    printf(_("Usage:\n  %s [OPTION]...\n\n"), progname);

    // Core server options
    printf(_("Options:\n"));
    printf(_("  -B NBUFFERS        number of shared buffers\n"));
    printf(_("  -c NAME=VALUE      set run-time parameter\n"));
    printf(_("  -C NAME            print value of run-time parameter, then exit\n"));
    printf(_("  -d 1-5             debugging level\n"));
    printf(_("  -D DATADIR         database directory\n"));
    printf(_("  -e                 use European date input format (DMY)\n"));
    printf(_("  -F                 turn fsync off\n"));
    printf(_("  -h HOSTNAME        host name or IP address to listen on\n"));
    printf(_("  -k DIRECTORY       Unix-domain socket location\n"));
#ifdef USE_SSL
    printf(_("  -l                 enable SSL connections\n"));
#endif
    printf(_("  -N MAX-CONNECT     maximum number of allowed connections\n"));
    printf(_("  -p PORT            port number to listen on\n"));
    printf(_("  -V, --version      output version information, then exit\n"));
    printf(_("  -?, --help         show this help, then exit\n"));

    // Developer options
    printf(_("\nDeveloper options:\n"));
    printf(_("  -f s|i|o|b|t|n|m|h forbid use of some plan types\n"));
    printf(_("  -O                 allow system table structure changes\n"));
    printf(_("  -P                 disable system indexes\n"));
    printf(_("  -T                 send SIGABRT to all backend processes if one dies\n"));

    // Single-user mode
    printf(_("\nOptions for single-user mode:\n"));
    printf(_("  --single           selects single-user mode (must be first argument)\n"));
    printf(_("  DBNAME             database name (defaults to user name)\n"));
    printf(_("  -E                 echo statement before execution\n"));

    // Bootstrap mode
    printf(_("\nOptions for bootstrapping mode:\n"));
    printf(_("  --boot             selects bootstrapping mode (must be first argument)\n"));
    printf(_("  --check            selects check mode (must be first argument)\n"));

    // Documentation references
    printf(_("\nPlease read the documentation for the complete list of run-time\n"
             "configuration settings and how to set them on the command line or in\n"
             "the configuration file.\n\n"
             "Report bugs to <%s>.\n"), PACKAGE_BUGREPORT);
    printf(_("%s home page: <%s>\n"), PACKAGE_NAME, PACKAGE_URL);
}
```