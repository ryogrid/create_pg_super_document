# generate_old_dump

## Location
[src/bin/pg_upgrade/dump.c:16-71](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/dump.c#L16-L71)

## Overview
Generates database dumps from the old PostgreSQL cluster during pg_upgrade operations, creating both global objects dump and individual database schema dumps.

## Definition

```c
void
generate_old_dump(void)
```
## Detailed Description
The  function is a critical component of PostgreSQL's pg_upgrade utility that creates comprehensive dumps of the old cluster's data structure before performing the upgrade. The function operates in two main phases:

1. **Global Objects Phase**: Creates a dump of cluster-wide objects (roles, tablespaces, databases, etc.) using pg_dumpall with the  option.

2. **Database Schema Phase**: Iterates through all databases in the old cluster and creates individual schema-only dumps using pg_dump with binary upgrade format. These dumps are executed in parallel for better performance.

The function uses binary upgrade mode () which preserves PostgreSQL internal identifiers (OIDs) that are crucial for maintaining object relationships during the upgrade process. All dumps are saved to the configured dump directory with standardized filenames.

## Parameters / Member Variables
This function takes no parameters as it operates on global cluster state variables:
- Uses  global variable to access source cluster information
- Uses  to locate the new PostgreSQL binaries
- Uses  for logging configuration and dump directory location

## Dependencies
- Functions called/Symbols referenced:
  -  - [Initialize](../I/Initialize.md) status reporting for global objects dump
  -  - Execute pg_dumpall for global objects
  -  - Generate connection options for old cluster
  -  - Verify successful completion of operations
  -  - [Initialize](../I/Initialize.md) progress reporting for database schemas
  -  - Execute pg_dump commands in parallel for each database
  - , , , ,  - [String](../S/String.md) buffer operations for connection strings
  -  - Log database processing status
  -  - Wait for parallel processes to complete
  -  - Finalize progress reporting

- Called from (representative examples):
  -  - Main upgrade orchestration function

## Notes and Other Information
- The function assumes the new cluster binaries (pg_dump, pg_dumpall) are compatible with the old cluster data
- Uses custom format dumps () for database schemas, which provides better performance and compression
- Implements parallel processing for database dumps to improve performance on multi-database clusters  
- Escapes connection strings properly to handle database names with special characters
- All dump files are created with standardized naming conventions using masks like 
- The  option ensures that all database object names are properly quoted to handle reserved words and special characters
- Binary upgrade mode preserves critical metadata that standard dumps would not include, such as OID assignments

## Simplified Source

```c
void generate_old_dump(void) {
    int dbnum;

    // Phase 1: Create dump of global objects (roles, tablespaces, etc.)
    prep_status("Creating dump of global objects");
    exec_prog(UTILITY_LOG_FILE, NULL, true, true,
              "\"%s/pg_dumpall\" %s --globals-only --quote-all-identifiers "
              "--binary-upgrade %s -f \"%s/%s\"",
              new_cluster.bindir, cluster_conn_opts(&old_cluster),
              log_opts.verbose ? "--verbose" : "",
              log_opts.dumpdir, GLOBALS_DUMP_FILE);
    check_ok();

    // Phase 2: Create schema dumps for each database
    prep_status_progress("Creating dump of database schemas");

    for (dbnum = 0; dbnum < old_cluster.dbarr.ndbs; dbnum++) {
        char sql_file_name[MAXPGPATH], log_file_name[MAXPGPATH];
        DbInfo *old_db = &old_cluster.dbarr.dbs[dbnum];
        PQExpBufferData connstr, escaped_connstr;

        // Build database connection string
        initPQExpBuffer(&connstr);
        appendPQExpBufferStr(&connstr, "dbname=");
        appendConnStrVal(&connstr, old_db->db_name);
        initPQExpBuffer(&escaped_connstr);
        appendShellString(&escaped_connstr, connstr.data);
        termPQExpBuffer(&connstr);

        // Generate filenames for this database
        pg_log(PG_STATUS, "%s", old_db->db_name);
        snprintf(sql_file_name, sizeof(sql_file_name), DB_DUMP_FILE_MASK, old_db->db_oid);
        snprintf(log_file_name, sizeof(log_file_name), DB_DUMP_LOG_FILE_MASK, old_db->db_oid);

        // Execute pg_dump in parallel for performance
        parallel_exec_prog(log_file_name, NULL,
                          "\"%s/pg_dump\" %s --schema-only --quote-all-identifiers "
                          "--binary-upgrade --format=custom %s --file=\"%s/%s\" %s",
                          new_cluster.bindir, cluster_conn_opts(&old_cluster),
                          log_opts.verbose ? "--verbose" : "",
                          log_opts.dumpdir, sql_file_name, escaped_connstr.data);

        termPQExpBuffer(&escaped_connstr);
    }

    // Wait for all parallel dump processes to complete
    while (reap_child(true) == true)
        ;

    end_progress_output();
    check_ok();
}
```