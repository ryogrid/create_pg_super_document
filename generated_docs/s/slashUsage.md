# slashUsage

## Location
[src/bin/psql/help.c:151-360](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/help.c#L151-L360)

## Overview
The slashUsage function displays comprehensive help information for all psql backslash commands, organizing them into logical categories for easy reference.

## Definition
void slashUsage(unsigned short int pager)

## Detailed Description
This function generates and displays detailed help text for all psql backslash (meta) commands. It builds the complete help output in a buffer, counts the lines for pagination purposes, and then displays the content using the appropriate output method. The help is organized into logical sections including General commands, Help commands, Query Buffer operations, Input/Output, Conditional statements, Informational commands, Large Objects, Formatting options, Connection management, Operating System interactions, and Variables. Some help text includes dynamic content showing current settings (e.g., HTML mode status, timing status).

## Parameters / Member Variables
- `pager`: Controls whether the output should be paginated. Non-zero values enable pagination using psql's pager settings

## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md) (buffer structure for building output)
  - [PQdb](../P/PQdb.md) (get current database name)
  - [initPQExpBuffer](../i/initPQExpBuffer.md) (initialize the output buffer)
  - HELP0/HELPN (macros for adding help text)
  - ON (macro for displaying on/off status)
  - [PageOutput](../P/PageOutput.md) (handle paginated output)
  - [ClosePager](../C/ClosePager.md) (close the pager when done)
  - [termPQExpBuffer](../t/termPQExpBuffer.md) (cleanup the buffer)
  - Various pset variables for current settings display
- Called from (representative examples):
  - [exec_command_slash_command_help](../e/exec_command_slash_command_help.md) (in src/bin/psql/command.c:3080, 3086)
  - [parse_psql_options](../p/parse_psql_options.md) (in src/bin/psql/startup.c:704)

## Notes and Other Information
- This function is part of psql's interactive help system
- The help content includes dynamic elements that show current psql settings
- Uses internationalization support through gettext macros
- The function counts output lines to determine if pagination is needed
- Conditional compilation sections (e.g., USE_READLINE) affect which commands are displayed
- Located at src/bin/psql/help.c:151-360
- The output includes comprehensive coverage of all psql meta-commands with syntax and brief descriptions

## Simplified Source

```c
void slashUsage(unsigned short int pager) {
    PQExpBufferData buf;
    int nlcount;
    FILE *output;
    char *currdb;

    currdb = PQdb(pset.db);
    initPQExpBuffer(&buf);

    // Build comprehensive help text for all backslash commands

    // General commands section
    HELP0("General\n");
    HELP0("  \\bind [PARAM]...       set query parameters\n");
    HELP0("  \\copyright             show PostgreSQL usage and distribution terms\n");
    HELP0("  \\crosstabview [COLUMNS] execute query and display result in crosstab\n");
    HELP0("  \\errverbose            show most recent error message at maximum verbosity\n");
    HELP0("  \\g [(OPTIONS)] [FILE]  execute query (and send result to file or |pipe)\n");
    HELP0("  \\gdesc                 describe result of query, without executing it\n");
    HELP0("  \\gexec                 execute query, then execute each value in its result\n");
    HELP0("  \\gset [PREFIX]         execute query and store result in psql variables\n");
    HELP0("  \\gx [(OPTIONS)] [FILE] as \\g, but forces expanded output mode\n");
    HELP0("  \\q                     quit psql\n");
    HELP0("  \\restrict RESTRICT_KEY enter restricted mode with provided key\n");
    HELP0("  \\unrestrict RESTRICT_KEY exit restricted mode if key matches\n");
    HELP0("  \\watch [[i=]SEC] [c=N] [m=MIN] execute query every SEC seconds\n");

    // Help commands section
    HELP0("Help\n");
    HELP0("  \\? [commands]          show help on backslash commands\n");
    HELP0("  \\? options             show help on psql command-line options\n");
    HELP0("  \\? variables           show help on special variables\n");
    HELP0("  \\h [NAME]              help on syntax of SQL commands, * for all commands\n");

    // Query Buffer section
    HELP0("Query Buffer\n");
    HELP0("  \\e [FILE] [LINE]       edit the query buffer (or file) with external editor\n");
    HELP0("  \\ef [FUNCNAME [LINE]]  edit function definition with external editor\n");
    HELP0("  \\ev [VIEWNAME [LINE]]  edit view definition with external editor\n");
    HELP0("  \\p                     show the contents of the query buffer\n");
    HELP0("  \\r                     reset (clear) the query buffer\n");
#ifdef USE_READLINE
    HELP0("  \\s [FILE]              display history or save it to file\n");
#endif
    HELP0("  \\w FILE                write query buffer to file\n");

    // Input/Output section
    HELP0("Input/Output\n");
    HELP0("  \\copy ...              perform SQL COPY with data stream to the client host\n");
    HELP0("  \\echo [-n] [STRING]    write string to standard output\n");
    HELP0("  \\i FILE                execute commands from file\n");
    HELP0("  \\ir FILE               as \\i, but relative to location of current script\n");
    HELP0("  \\o [FILE]              send all query results to file or |pipe\n");
    HELP0("  \\qecho [-n] [STRING]   write string to \\o output stream\n");
    HELP0("  \\warn [-n] [STRING]    write string to standard error\n");

    // Add remaining sections: Conditional, Informational, Large Objects,
    // Formatting, Connection, Operating System, Variables
    // ... (abbreviated for brevity - would include all help sections)

    // Count lines for pagination
    nlcount = 0;
    for (const char *ptr = buf.data; *ptr; ptr++) {
        if (*ptr == '\n')
            nlcount++;
    }

    // Display output with appropriate pagination
    output = PageOutput(nlcount, pager ? &(pset.popt.topt) : NULL);
    fputs(buf.data, output);
    ClosePager(output);

    termPQExpBuffer(&buf);
}
```