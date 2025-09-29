# usage

## Location
[src/bin/pgbench/pgbench.c:870-950](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L870-L950)

## Overview
A static function that prints comprehensive help text for the initdb command, displaying all available command-line options and their descriptions.

## Definition
static void usage(const char *progname)

## Detailed Description
The usage function in initdb serves as the help system for the PostgreSQL database cluster initialization utility. It provides a comprehensive overview of all available command-line options, organized into logical groups including authentication options, locale settings, WAL configuration, debugging options, and general utilities. The function uses internationalization (i18n) support through the _() macro to ensure help text can be displayed in multiple languages. This function is essential for user experience, providing clear guidance on how to properly configure a new PostgreSQL database cluster during initialization.

## Parameters / Member Variables
- progname: The name of the program (typically "initdb") used in the usage examples and output formatting

## Dependencies
- Functions called/Symbols referenced:
  - printf (standard C library function for formatted output)
  - _() (internationalization macro for translatable strings)
  - PACKAGE_BUGREPORT (macro containing bug report contact information)
  - PACKAGE_NAME (macro containing the package name)
  - PACKAGE_URL (macro containing the project homepage URL)

- Called from (representative examples):
  - [main](../m/main.md) (when help is requested via command line options)

## Notes and Other Information
- Supports internationalization through gettext-style _() macros for translatable help text
- Organizes options into logical groups: basic options, authentication, locale settings, less common options, and general help
- Provides detailed explanations for complex options like locale configuration and WAL settings
- Includes contact information for bug reports and project homepage
- Essential for user experience and proper database cluster configuration guidance
- Uses consistent formatting and spacing to ensure readability across different terminal widths

## Simplified Source

```c
// Simplified version of usage
static void usage(const char *progname) {
    // Print program description and basic usage
    printf(_("%s initializes a PostgreSQL database cluster.\n\n"), progname);
    printf(_("Usage:\n"));
    printf(_("  %s [OPTION]... [DATADIR]\n"), progname);

    // Main options section
    printf(_("\nOptions:\n"));

    // Authentication options
    printf(_("  -A, --auth=METHOD         default authentication method for local connections\n"));
    printf(_("      --auth-host=METHOD    default authentication method for local TCP/IP connections\n"));
    printf(_("      --auth-local=METHOD   default authentication method for local-socket connections\n"));

    // Core database configuration
    printf(_(" [-D, --pgdata=]DATADIR     location for this database cluster\n"));
    printf(_("  -E, --encoding=ENCODING   set default encoding for new databases\n"));
    printf(_("  -g, --allow-group-access  allow group read/execute on data directory\n"));

    // Locale and ICU settings
    printf(_("      --icu-locale=LOCALE   set ICU locale ID for new databases\n"));
    printf(_("      --locale=LOCALE       set default locale for new databases\n"));
    printf(_("      --locale-provider={builtin|libc|icu}\n"));

    // Additional configuration options
    printf(_("  -k, --data-checksums      use data page checksums\n"));
    printf(_("  -T, --text-search-config=CFG  default text search configuration\n"));
    printf(_("  -U, --username=NAME       database superuser name\n"));
    printf(_("  -W, --pwprompt            prompt for a password for the new superuser\n"));
    printf(_("  -X, --waldir=WALDIR       location for the write-ahead log directory\n"));

    // Less common options
    printf(_("\nLess commonly used options:\n"));
    printf(_("  -c, --set NAME=VALUE      override default setting for server parameter\n"));
    printf(_("  -d, --debug               generate lots of debugging output\n"));
    printf(_("  -n, --no-clean            do not clean up after errors\n"));
    printf(_("  -N, --no-sync             do not wait for changes to be written safely to disk\n"));
    printf(_("  -s, --show                show internal settings, then exit\n"));

    // Help and version options
    printf(_("\nOther options:\n"));
    printf(_("  -V, --version             output version information, then exit\n"));
    printf(_("  -?, --help                show this help, then exit\n"));

    // Footer information
    printf(_("\nIf the data directory is not specified, the environment variable PGDATA\n"
             "is used.\n"));
    printf(_("\nReport bugs to <%s>.\n"), PACKAGE_BUGREPORT);
    printf(_("%s home page: <%s>\n"), PACKAGE_NAME, PACKAGE_URL);
}
```

Key simplifications made:
- Consolidated related printf statements with clearer grouping comments
- Removed some of the more verbose multi-line option descriptions for brevity
- Maintained the essential structure and all critical options
- Simplified complex locale option descriptions while preserving core functionality
- Kept internationalization support through _() macros
- Preserved the logical flow: header → main options → less common options → help/version → footer