# get_su_pwd

## Location
[src/bin/initdb/initdb.c:1639-1697](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L1639-L1697)

## Overview
The  function obtains the superuser password for database initialization, either by prompting the user interactively or reading from a specified password file.

## Definition

```c
static void
get_su_pwd(void)
```
## Detailed Description
This function is a critical component of the PostgreSQL initdb utility that handles secure password acquisition for the superuser account during database cluster initialization. It supports two modes of operation:

1. **Interactive mode (pwprompt = true)**: Prompts the user to enter the password twice via terminal input using masked prompts, then validates that both entries match to prevent typos.

2. **File mode (pwprompt = false)**: Reads the password from a file specified by the global  variable. The function strips any trailing carriage return/line feed characters to ensure clean password handling.

The function performs comprehensive error handling including file access validation, empty file detection, and read error reporting. Upon successful password acquisition, it stores the result in the global  variable for later use during database initialization.

## Parameters / Member Variables
- Uses global variables:
  - : Boolean flag determining input method
  - : Path to password file when not prompting
  - : Output variable storing the acquired password

## Dependencies
- Functions called/Symbols referenced:
  - : Interactive password input with masking
  - : File opening for password file access
  - : PostgreSQL utility for reading lines from files
  - : PostgreSQL utility for removing line endings
- Called from (representative examples):
  - : Primary initdb execution flow
  - : Authentication configuration context

## Notes and Other Information
- The function includes a security consideration note about file permissions on Windows systems where traditional Unix permissions may not apply
- Password confirmation is only performed in interactive mode to prevent user input errors
- The function terminates the program (exit(1)) if password confirmation fails
- Memory management is handled appropriately with  for the confirmation password
- Error messages are internationalized using the  macro for localization support

## Simplified Source

```c
static void get_su_pwd(void) {
    char *pwd1;

    if (pwprompt) {
        // Interactive mode: prompt for password twice
        char *pwd2;

        printf("\n");
        fflush(stdout);
        pwd1 = simple_prompt("Enter new superuser password: ", false);
        pwd2 = simple_prompt("Enter it again: ", false);

        if (strcmp(pwd1, pwd2) != 0) {
            fprintf(stderr, _("Passwords didn't match.\n"));
            exit(1);
        }
        free(pwd2);
    } else {
        // File mode: read password from file
        FILE *pwf = fopen(pwfilename, "r");

        if (!pwf)
            pg_fatal("could not open file \"%s\" for reading: %m", pwfilename);

        pwd1 = pg_get_line(pwf, NULL);
        if (!pwd1) {
            if (ferror(pwf))
                pg_fatal("could not read password from file \"%s\": %m", pwfilename);
            else
                pg_fatal("password file \"%s\" is empty", pwfilename);
        }
        fclose(pwf);

        // Strip any trailing newlines
        (void) pg_strip_crlf(pwd1);
    }

    superuser_password = pwd1;
}
```