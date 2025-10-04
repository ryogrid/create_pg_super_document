# run_ssl_passphrase_command

## Location
[src/backend/libpq/be-secure-common.c:40-113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-common.c#L40-L113)

## Overview
Executes the configured SSL passphrase command to retrieve passwords for encrypted SSL keys, substituting placeholders with provided prompts.

## Definition
```c
int run_ssl_passphrase_command(const char *prompt, bool is_server_start, char *buf, int size)
```

## Detailed Description
This function executes the external command specified by the `ssl_passphrase_command` configuration parameter to retrieve passphrases for encrypted SSL private keys. The command is run with placeholder substitution where `%p` in the command string is replaced with the provided prompt. The function handles secure execution of the external process, reads the passphrase from its stdout, and performs proper cleanup including memory zeroing for security. Error handling varies based on whether this is called during server startup or runtime operation.

## Parameters / Member Variables
- `prompt`: The prompt string to substitute for %p placeholder in the SSL passphrase command
- `is_server_start`: Boolean flag determining error message log level (ERROR for startup, LOG for runtime)
- `buf`: Buffer to store the retrieved passphrase
- `size`: Size of the buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [replace_percent_placeholders](replace_percent_placeholders.md)
  - [OpenPipeStream](../O/OpenPipeStream.md)
  - [ClosePipeStream](../C/ClosePipeStream.md)
  - [explicit_bzero](../e/explicit_bzero.md)
  - [wait_result_to_str](../w/wait_result_to_str.md)
  - [errdetail_internal](../e/errdetail_internal.md)
  - [pg_strip_crlf](../p/pg_strip_crlf.md)
- Called from (representative examples):
  - [ssl_external_passwd_cb](../s/ssl_external_passwd_cb.md)

## Notes and Other Information
- The function performs secure memory handling by using explicit_bzero() to clear sensitive data
- Trailing newlines and carriage returns are automatically stripped from the command output
- Error logging level is contextual: ERROR during server startup, LOG during runtime operations
- The function returns the length of the retrieved passphrase
- Proper error handling ensures the pipe is closed and memory is cleaned up even on failure

## Simplified Source

```c
int run_ssl_passphrase_command(const char *prompt, bool is_server_start, char *buf, int size) {
    int loglevel = is_server_start ? ERROR : LOG;
    char *command;
    FILE *fh;
    size_t len = 0;

    // Initialize buffer and validate inputs
    Assert(prompt);
    Assert(size > 0);
    buf[0] = '\0';

    // Build command with placeholder substitution
    command = replace_percent_placeholders(ssl_passphrase_command,
                                         "ssl_passphrase_command", "p", prompt);

    // Execute command and read passphrase
    fh = OpenPipeStream(command, "r");
    if (fh == NULL) {
        ereport(loglevel, (errcode_for_file_access(),
                          errmsg("could not execute command \"%s\": %m", command)));
        goto error;
    }

    // Read result from command output
    if (!fgets(buf, size, fh)) {
        if (ferror(fh)) {
            explicit_bzero(buf, size);
            ereport(loglevel, (errcode_for_file_access(),
                              errmsg("could not read from command \"%s\": %m", command)));
            goto error;
        }
    }

    // Close pipe and handle errors
    int pclose_rc = ClosePipeStream(fh);
    if (pclose_rc != 0) {
        explicit_bzero(buf, size);
        if (pclose_rc != -1) {
            char *reason = wait_result_to_str(pclose_rc);
            ereport(loglevel, (errcode_for_file_access(),
                              errmsg("command \"%s\" failed", command),
                              errdetail_internal("%s", reason)));
            pfree(reason);
        }
        goto error;
    }

    // Strip trailing whitespace and return length
    len = pg_strip_crlf(buf);

error:
    pfree(command);
    return len;
}
```