# dumpDatabases

## Location
[src/bin/pg_dump/pg_dumpall.c:1581-1676](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dumpall.c#L1581-L1676)

## Overview
Orchestrates the dumping of all databases by iterating through allowed databases and invoking pg_dump for each one with appropriate options.

## Definition

```c
structed stem of connection
	 * string.
	 */
	appendPQExpBuffer(&connstrbuf, "%s dbname=", connstr);
```
## Detailed Description
This function is responsible for dumping the contents of all databases in a PostgreSQL cluster as part of the pg_dumpall utility. It queries the system catalog to find all databases that allow connections (datallowconn is true) and are not marked with connection limit -2. The function processes databases in a specific order: template1 first, then all other databases alphabetically. This ordering prevents issues that could arise when using the --clean option, such as trying to drop the currently connected database.

For each database, the function determines the appropriate pg_dump options based on whether it's a system database (template1/postgres) and whether the --clean option was specified. System databases are handled specially since they're assumed to already exist in the target installation. The function skips template0 (even if marked as allowing connections) and any explicitly excluded databases.

## Parameters / Member Variables
- : Active PostgreSQL database connection used to query the system catalog for database information

## Dependencies
- Functions called/Symbols referenced:
  - [executeQuery](../e/executeQuery.md) (executes SQL queries)
  - [simple_string_list_member](../s/simple_string_list_member.md) (checks database exclusion list)
  - pg_log_info (logging utility)
  - [sanitize_line](../s/sanitize_line.md) (sanitizes database names for output)
  - [runPgDump](../r/runPgDump.md) (executes pg_dump for individual databases)
  - PG_BINARY_A (file mode constant)
  - fopen (file operations)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_dumpall.c at line 646)

## Notes and Other Information
- Skips databases with datallowconn=false or datconnlimit=-2 to avoid connection failures
- Always skips template0 regardless of its datallowconn setting
- Processes template1 first, then other databases alphabetically to avoid --clean operation conflicts
- For system databases (template1, postgres), uses different options depending on --clean flag
- Handles file output by temporarily closing and reopening output files for each database dump
- Fatal error occurs if pg_dump fails on any database, ensuring data consistency
- Special handling for database exclusion list allows selective database dumping

## Simplified Source

```c
static void dumpDatabases(PGconn *conn)
{
    PGresult *res;
    int i;

    // Query all databases that allow connections, ordered specially:
    // template1 first, then others alphabetically
    res = executeQuery(conn,
                      "SELECT datname "
                      "FROM pg_database d "
                      "WHERE datallowconn AND datconnlimit != -2 "
                      "ORDER BY (datname <> 'template1'), datname");

    // Print header if databases found
    if (PQntuples(res) > 0)
        fprintf(OPF, "--\n-- Databases\n--\n\n");

    // Process each database
    for (i = 0; i < PQntuples(res); i++)
    {
        char *dbname = PQgetvalue(res, i, 0);
        char *sanitized;
        const char *create_opts;
        int ret;

        // Skip template0 database
        if (strcmp(dbname, "template0") == 0)
            continue;

        // Skip explicitly excluded databases
        if (simple_string_list_member(&database_exclude_names, dbname))
        {
            pg_log_info("excluding database \"%s\"", dbname);
            continue;
        }

        pg_log_info("dumping database \"%s\"", dbname);

        // Print database header comment
        sanitized = sanitize_line(dbname, true);
        fprintf(OPF, "--\n-- Database \"%s\" dump\n--\n\n", sanitized);
        free(sanitized);

        // Determine pg_dump options based on database type
        if (strcmp(dbname, "template1") == 0 || strcmp(dbname, "postgres") == 0)
        {
            // System databases - handle differently based on clean option
            if (output_clean)
                create_opts = "--clean --create";
            else
            {
                create_opts = "";
                // Need explicit connect since pg_dump won't emit it
                fprintf(OPF, "\\connect %s\n\n", dbname);
            }
        }
        else
            create_opts = "--create";

        // Handle file output - close temporarily for pg_dump
        if (filename)
            fclose(OPF);

        // Run pg_dump for this database
        ret = runPgDump(dbname, create_opts);
        if (ret != 0)
            pg_fatal("pg_dump failed on database \"%s\", exiting", dbname);

        // Reopen output file if needed
        if (filename)
        {
            OPF = fopen(filename, PG_BINARY_A);
            if (!OPF)
                pg_fatal("could not re-open the output file \"%s\": %m", filename);
        }
    }

    PQclear(res);
}
```