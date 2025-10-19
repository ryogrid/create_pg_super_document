# check_loadable_libraries

## Location
[src/bin/pg_upgrade/function.c:146-219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/function.c#L146-L219)

## Overview
Verifies that all required libraries from the old PostgreSQL cluster are present and compatible in the new cluster by attempting to LOAD each library.

## Definition
```c
void check_loadable_libraries(void)
```

## Detailed Description
This function performs a critical compatibility check during pg_upgrade by testing each library collected by get_loadable_libraries() in the new PostgreSQL installation. It connects to the template1 database in the new cluster and systematically attempts to execute LOAD commands for each unique library. The libraries are first sorted using library_name_compare() to ensure consistent ordering and avoid redundant probes. If any library fails to load, the function records the failure details in a loadable_libraries.txt file and terminates the upgrade process with a fatal error, providing guidance to the user on how to resolve missing library issues.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [connectToServer](connectToServer.md) (connect to new cluster)
  - [prep_status](../p/prep_status.md) (status reporting)
  - qsort (sorting libraries)
  - [library_name_compare](../l/library_name_compare.md) (comparison function)
  - [PQescapeStringConn](../P/PQescapeStringConn.md), PQexec, PQclear, PQfinish (PostgreSQL operations)
  - fopen_priv (secure file opening)
  - [pg_fatal](../p/pg_fatal.md), pg_log (error reporting)
  - [check_ok](check_ok.md) (success reporting)
  - [LibraryInfo](../L/LibraryInfo.md) (structure type)
  - PGRES_COMMAND_OK, PG_REPORT (constants)
- Called from (representative examples):
  - [check_new_cluster](check_new_cluster.md)

## Notes and Other Information
- Uses template1 database for testing library loads in the new cluster
- Eliminates duplicate library tests by comparing with the previous library name after sorting
- Creates loadable_libraries.txt file only when failures occur
- Provides detailed error messages including specific database names where libraries were referenced
- Terminates pg_upgrade process immediately upon detecting any missing libraries
- Sorting ensures reproducible behavior and proper dependency handling between libraries
- Uses PQescapeStringConn for safe SQL command construction with library paths

## Simplified Source

```c
void
check_loadable_libraries(void)
{
    PGconn *connection = connectToServer(&new_cluster, "template1");
    int library_index;
    bool has_load_failure = false;
    FILE *error_log = NULL;
    char log_file_path[MAXPGPATH];

    prep_status("Checking for presence of required libraries");

    // Set up error log file path
    snprintf(log_file_path, sizeof(log_file_path), "%s/%s",
             log_opts.basedir, "loadable_libraries.txt");

    // Sort libraries to avoid duplicate tests and ensure consistent ordering
    qsort(os_info.libraries, os_info.num_libraries,
          sizeof(LibraryInfo), library_name_compare);

    // Test each unique library by attempting to LOAD it
    for (library_index = 0; library_index < os_info.num_libraries; library_index++)
    {
        char *library_name = os_info.libraries[library_index].name;
        int name_length = strlen(library_name);
        char load_command[7 + 2 * MAXPGPATH + 1];
        PGresult *result;

        // Skip if this is the same library as the previous one (avoid duplicates)
        if (library_index == 0 ||
            strcmp(library_name, os_info.libraries[library_index - 1].name) != 0)
        {
            // Build and execute LOAD command with proper SQL escaping
            strcpy(load_command, "LOAD '");
            PQescapeStringConn(connection, load_command + strlen(load_command),
                               library_name, name_length, NULL);
            strcat(load_command, "'");

            result = PQexec(connection, load_command);

            // Check if LOAD command failed
            if (PQresultStatus(result) != PGRES_COMMAND_OK)
            {
                has_load_failure = true;

                // Open error log file if not already open
                if (error_log == NULL)
                    error_log = fopen_priv(log_file_path, "w");

                // Log the library load failure
                fprintf(error_log, _("could not load library \"%s\": %s"),
                        library_name, PQerrorMessage(connection));
            }
            else
                has_load_failure = false;

            PQclear(result);
        }

        // Log which database referenced this failed library
        if (has_load_failure)
            fprintf(error_log, _("In database: %s\n"),
                    old_cluster.dbarr.dbs[os_info.libraries[library_index].dbnum].db_name);
    }

    PQfinish(connection);

    // Handle results: either report success or fatal error
    if (error_log)
    {
        fclose(error_log);
        pg_log(PG_REPORT, "fatal");
        pg_fatal("Your installation references loadable libraries that are missing from the\n"
                 "new installation. You can add these libraries to the new installation,\n"
                 "or remove the functions using them from the old installation. A list of\n"
                 "problem libraries is in the file:\n    %s", log_file_path);
    }
    else
        check_ok();
}
```