# SetShellResultVariables

## Location
src/bin/psql/common.c: 501 - 522

## Overview
SetShellResultVariables updates psql's special variables to track the success and exit status of shell commands executed within psql.

## Definition


## Detailed Description
SetShellResultVariables is a public function that manages psql's special variables for tracking shell command execution results. It takes a wait status value (as returned by system calls like wait(), waitpid(), pclose(), or system()) and converts it into user-friendly psql variables. The function sets SHELL_ERROR to "false" if the command succeeded (wait_result == 0) or "true" if it failed, and sets SHELL_EXIT_CODE to the actual exit code extracted from the wait status using a helper function. This provides a consistent interface for psql scripts to check the success of shell commands and access their exit codes for conditional logic.

## Parameters / Member Variables
- : Integer wait status as returned by wait(2), waitpid(2), pclose(3), or system(3) functions

## Dependencies
- Functions called/Symbols referenced:
  - SetVariable (psql variable management function)
  - wait_result_to_exit_code (helper function to extract exit code from wait status)
  - snprintf (standard C library function)
- Global variables accessed:
  - pset.vars (psql variable storage)
- Called from:
  - exec_command_write (src/bin/psql/command.c:2823)
  - do_shell (src/bin/psql/command.c:5312) 
  - CloseGOutput (src/bin/psql/common.c:116)
  - setQFout (src/bin/psql/common.c:145)
  - do_copy (src/bin/psql/copy.c:394)

## Notes and Other Information
This function is part of psql's shell integration system, providing feedback mechanisms for commands that interact with the operating system. Unlike SetResultVariables which handles SQL query results, this function specifically deals with external command execution. The distinction between wait_result and exit_code is important: wait_result contains the raw status from system calls which may include signal information, while the exit_code extracts just the program's exit status. The SHELL_ERROR and SHELL_EXIT_CODE variables enable psql scripts to implement robust error handling for shell operations, making it possible to write conditional logic based on external command success or failure.