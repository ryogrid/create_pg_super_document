# exec_command_write

## Location
src/bin/psql/command.c: 2751 - 2852

## Overview
Implements the psql \w command, which writes the contents of the query buffer to a file or pipes it to a shell command.

## Definition
static backslashResult exec_command_write(PsqlScanState scan_state, bool active_branch, const char *cmd, PQExpBuffer query_buf, PQExpBuffer previous_buf)

## Detailed Description
This function handles the execution of the \w command in psql, which writes the current query buffer contents to a specified file or pipes it to a shell command. The function can write to regular files or pipe to shell commands (when the filename starts with '|'). It prioritizes the current query buffer, but falls back to the previous query buffer if the current one is empty. The function includes comprehensive error handling for file operations and properly manages pipe processes including signal handling.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer used for parsing command arguments
- `active_branch`: Boolean indicating whether the command should be executed (true) or just parsed (false)
- `cmd`: String containing the command name ("w") for error reporting purposes
- `query_buf`: PQExpBuffer containing the current query to be written
- `previous_buf`: PQExpBuffer containing the previous query as fallback

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option: Parses the filename/pipe argument
  - expand_tilde: Expands ~ in file paths
  - canonicalize_path_enc: Normalizes file paths with proper encoding
  - disable_sigpipe_trap/restore_sigpipe_trap: Manages signal handling for pipes
  - popen/pclose: Opens and closes pipe processes
  - fopen/fclose: Opens and closes regular files
  - SetShellResultVariables: Sets result variables for pipe commands
  - wait_result_to_str: Converts process exit codes to strings
  - ignore_slash_filepipe: Skips parsing when in inactive branch
- Called from (representative examples):
  - exec_command: Main command dispatcher in psql

## Notes and Other Information
- Supports both file output and pipe output (filename starting with '|')
- Automatically chooses between current and previous query buffers
- Includes proper signal handling for pipe operations to avoid SIGPIPE issues
- Uses platform-appropriate file path handling with encoding considerations
- Returns PSQL_CMD_SKIP_LINE on success, PSQL_CMD_ERROR on failure
- Memory management: Properly frees allocated filename string
- Error reporting includes system error messages (%m) for file operations