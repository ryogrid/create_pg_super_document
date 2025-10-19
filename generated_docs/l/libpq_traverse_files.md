# libpq_traverse_files

## Location
[src/bin/pg_rewind/libpq_source.c:233-325](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/libpq_source.c#L233-L325)

## Overview
Recursively traverses the PostgreSQL data directory via a libpq connection to retrieve a complete list of all files and directories, including tablespace symbolic links.

## Definition

```c
static void
libpq_traverse_files(rewind_source *source, process_file_callback_t callback)
```
## Detailed Description
This function implements the file traversal functionality for the libpq-based rewind source in pg_rewind. It uses a complex recursive SQL query to enumerate all files and directories in the PostgreSQL data directory remotely through a database connection. The query leverages PostgreSQL's built-in functions  and  to build a complete directory tree.

The function handles special cases for tablespaces by joining with the  catalog to resolve tablespace symbolic links. It distinguishes between absolute path tablespaces (true symbolic links) and in-place tablespaces (directories with relative paths) within the  directory.

For each file or directory found, the function determines its type (regular file, directory, or symbolic link) and invokes the provided callback function with the file information.

## Parameters / Member Variables
- `*source`: Pointer to the rewind_source structure containing the libpq connection information
- `callback`: Function pointer to the callback that will be invoked for each file/directory found during traversal
## Dependencies
- Functions called/Symbols referenced:
  - [PQexec](../P/PQexec.md) (executes the recursive directory listing SQL query)
  - [PQresultStatus](../P/PQresultStatus.md) (checks query execution status)
  - [PQresultErrorMessage](../P/PQresultErrorMessage.md) (retrieves error messages on query failure)
  - [PQnfields](../P/PQnfields.md) (validates result set structure)
  - [PQntuples](../P/PQntuples.md) (gets number of result rows)
  - [PQgetisnull](../P/PQgetisnull.md) (checks for NULL values in result set)
  - [PQgetvalue](../P/PQgetvalue.md) (retrieves column values from result set)
  - [PQclear](../P/PQclear.md) (frees result set memory)
  - is_absolute_path (determines if tablespace path is absolute)
  - [pg_fatal](../p/pg_fatal.md) (reports fatal errors)
- Called from:
  - [init_libpq_source](../i/init_libpq_source.md) (as part of libpq_source function table initialization)

## Notes and Other Information
- The function uses a sophisticated WITH RECURSIVE SQL query that first lists the root directory contents, then recursively expands subdirectories
- Special handling is implemented for PostgreSQL tablespaces in the  directory
- Files that are removed during query execution are gracefully ignored (NULL size check)
- The query excludes hidden files and directories via the  parameters
- Custom symbolic links in the data directory are not handled correctly due to lack of a general backend function for retrieving symbolic link targets
- This is a static function used internally within the libpq_source.c module as part of the pg_rewind tool's remote file system access capabilities

## Simplified Source

```c
static void
libpq_traverse_files(rewind_source *source, process_file_callback_t callback)
{
    PGconn *conn = ((libpq_source *) source)->conn;
    PGresult *res;
    const char *sql;
    int i;

    // Build recursive SQL query to list all files in data directory
    sql = "WITH RECURSIVE files (path, filename, size, isdir) AS (\n"
          "  SELECT '' AS path, filename, size, isdir FROM\n"
          "  (SELECT pg_ls_dir('.', true, false) AS filename) AS fn,\n"
          "        pg_stat_file(fn.filename, true) AS this\n"
          "  UNION ALL\n"
          "  SELECT parent.path || parent.filename || '/' AS path,\n"
          "         fn, this.size, this.isdir\n"
          "  FROM files AS parent,\n"
          "       pg_ls_dir(parent.path || parent.filename, true, false) AS fn,\n"
          "       pg_stat_file(parent.path || parent.filename || '/' || fn, true) AS this\n"
          "       WHERE parent.isdir = 't'\n"
          ")\n"
          "SELECT path || filename, size, isdir,\n"
          "       pg_tablespace_location(pg_tablespace.oid) AS link_target\n"
          "FROM files\n"
          "LEFT OUTER JOIN pg_tablespace ON files.path = 'pg_tblspc/'\n"
          "                             AND oid::text = files.filename\n";

    // Execute the query
    res = PQexec(conn, sql);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
        pg_fatal("could not fetch file list: %s", PQresultErrorMessage(res));

    // Validate result structure
    if (PQnfields(res) != 4)
        pg_fatal("unexpected result set while fetching file list");

    // Process each file/directory
    for (i = 0; i < PQntuples(res); i++)
    {
        char *path;
        int64 filesize;
        bool isdir;
        char *link_target;
        file_type_t type;

        // Skip files removed during query execution
        if (PQgetisnull(res, i, 1))
            continue;

        // Extract file information
        path = PQgetvalue(res, i, 0);
        filesize = atol(PQgetvalue(res, i, 1));
        isdir = (strcmp(PQgetvalue(res, i, 2), "t") == 0);
        link_target = PQgetvalue(res, i, 3);

        // Determine file type
        if (link_target[0])
        {
            // Handle tablespace links
            if (is_absolute_path(link_target))
                type = FILE_TYPE_SYMLINK;
            else
                type = FILE_TYPE_DIRECTORY;  // In-place tablespace
        }
        else if (isdir)
            type = FILE_TYPE_DIRECTORY;
        else
            type = FILE_TYPE_REGULAR;

        // Call callback for this file
        callback(path, type, filesize, link_target);
    }

    PQclear(res);
}
```