# do_edit

## Location
[src/bin/psql/command.c:4185-4379](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L4185-L4379)

## Overview
Implements psql's \e command functionality by managing the complete workflow of editing query content in an external editor, including temporary file creation, editor invocation, and result processing.

## Definition
```c
static bool do_edit(const char *filename_arg, PQExpBuffer query_buf, int lineno, bool discard_on_quit, bool *edited)
```

## Detailed Description
This function orchestrates the entire editing process for psql's \e command. When no filename is provided, it creates a temporary file containing the current query buffer content, invokes the configured editor, and then reads the modified content back into the query buffer. The function includes sophisticated logic to detect whether the file was actually modified by comparing file size and modification timestamps before and after editing.

The function handles platform-specific temporary directory management, ensures proper file permissions for temporary files, and includes comprehensive error handling throughout the editing workflow. It can optionally clear the query buffer if editing was cancelled or the file wasn't modified.

## Parameters / Member Variables
- `filename_arg`: Optional filename to edit; if NULL, creates a temporary file with query buffer content
- `query_buf`: PQExpBuffer containing the current query that will be edited and potentially replaced
- `lineno`: Line number where the editor should position the cursor (passed to editFile)
- `discard_on_quit`: If true, clears the query buffer when the file wasn't modified
- `edited`: Output parameter set to true if the query buffer was successfully replaced with edited content

## Dependencies
- Functions called/Symbols referenced:
  - getenv() (for TMPDIR on Unix)
  - GetTempPath() (for temp directory on Windows)
  - getpid() (for unique temporary filename)
  - open(), fdopen() (for temporary file creation)
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)() (to ensure newline termination)
  - fwrite(), fclose() (for writing query to temporary file)
  - utime() (to set file modification time)
  - [stat](../s/stat.md)() (to check file modification before/after editing)
  - [editFile](../e/editFile.md)() (to invoke the external editor)
  - fopen(), fgets() (to read edited content back)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md)(), appendPQExpBufferStr() (for query buffer management)
  - remove() (to clean up temporary file)
- Called from:
  - [exec_command_edit](../e/exec_command_edit.md) (at src/bin/psql/command.c:1149)
  - [exec_command_ef_ev](../e/exec_command_ef_ev.md) (at src/bin/psql/command.c:1266)

## Notes and Other Information
- Creates temporary files with pattern 'psql.edit.[pid].sql' in system temp directory
- Uses O_EXCL flag when creating temporary files to prevent race conditions
- Sets temporary file permissions to 0600 (owner read/write only) for security
- Artificially sets temporary file modification time 2 seconds in the past to reliably detect quick edits
- Forces newline termination of content sent to editor for consistent formatting
- Detects file modification by comparing both size and modification time
- Handles platform differences: Unix uses TMPDIR env var and '/' separators, Windows uses GetTempPath()
- Static function, only accessible within the command.c source file
- Returns true on success, false on any error during the editing process
- Comprehensive error handling with cleanup of temporary files on failure

## Simplified Source

```c
static bool do_edit(const char *filename_arg, PQExpBuffer query_buf,
                   int lineno, bool discard_on_quit, bool *edited) {
    char fnametmp[MAXPGPATH];
    FILE *stream = NULL;
    const char *fname;
    bool error = false;
    struct stat before, after;

    if (filename_arg) {
        fname = filename_arg;
    } else {
        // Create temporary file in system temp directory
        const char *tmpdir = getenv("TMPDIR");
        if (!tmpdir) tmpdir = "/tmp";

        snprintf(fnametmp, sizeof(fnametmp), "%s/psql.edit.%d.sql",
                tmpdir, (int) getpid());
        fname = fnametmp;

        // Create temp file and write query buffer content
        int fd = open(fname, O_WRONLY | O_CREAT | O_EXCL, 0600);
        if (fd != -1)
            stream = fdopen(fd, "w");

        if (fd == -1 || !stream) {
            pg_log_error("could not open temporary file \"%s\": %m", fname);
            error = true;
        } else {
            // Force newline termination for editor
            unsigned int ql = query_buf->len;
            if (ql > 0 && query_buf->data[ql - 1] != '\n') {
                appendPQExpBufferChar(query_buf, '\n');
                ql++;
            }

            // Write content and close file
            if (fwrite(query_buf->data, 1, ql, stream) != ql ||
                fclose(stream) != 0) {
                pg_log_error("%s: %m", fname);
                error = true;
            }
        }
    }

    // Get file stats before editing
    if (!error && stat(fname, &before) != 0) {
        pg_log_error("%s: %m", fname);
        error = true;
    }

    // Launch external editor
    if (!error)
        error = !editFile(fname, lineno);

    // Get file stats after editing
    if (!error && stat(fname, &after) != 0) {
        pg_log_error("%s: %m", fname);
        error = true;
    }

    // Check if file was modified (size or time changed)
    if (!error &&
        (before.st_size != after.st_size || before.st_mtime != after.st_mtime)) {

        // Read edited content back into query buffer
        stream = fopen(fname, PG_BINARY_R);
        if (!stream) {
            pg_log_error("%s: %m", fname);
            error = true;
        } else {
            char line[1024];
            resetPQExpBuffer(query_buf);

            while (fgets(line, sizeof(line), stream) != NULL)
                appendPQExpBufferStr(query_buf, line);

            if (ferror(stream)) {
                pg_log_error("%s: %m", fname);
                error = true;
                resetPQExpBuffer(query_buf);
            } else if (edited) {
                *edited = true;
            }
            fclose(stream);
        }
    } else {
        // File not modified - discard query buffer if requested
        if (discard_on_quit)
            resetPQExpBuffer(query_buf);
    }

    // Clean up temporary file
    if (!filename_arg && remove(fname) == -1) {
        pg_log_error("%s: %m", fname);
        error = true;
    }

    return !error;
}
```