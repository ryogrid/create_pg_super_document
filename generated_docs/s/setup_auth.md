setup_auth

## Overview
Sets up the authentication system by securing the pg_authid table and optionally setting the superuser password during database cluster initialization.

## Definition
static void setup_auth(FILE *cmdfd)

## Detailed Description
This function configures the authentication and authorization system for the PostgreSQL database cluster. It performs two critical security operations:

1. Revokes all public access to the pg_authid table, ensuring that password hashes and other sensitive authentication information are not publicly readable. This is a crucial security measure as the pg_authid table contains password hashes and other sensitive user authentication data.

2. If a superuser password was provided during initdb (via the --pwprompt or --pwfile options), it sets the password for the database superuser account using the ALTER USER command.

The function uses the PG_CMD_PUTS and PG_CMD_PRINTF macros to send SQL commands to the backend process that is running in bootstrap mode. The superuser password is properly escaped using the escape_quotes function to prevent SQL injection and handle special characters safely.

## Parameters / Member Variables
- cmdfd: File descriptor for sending commands to the backend process (though this parameter appears to be unused in the current implementation, as the function uses PG_CMD macros)

## Dependencies
- Functions called/Symbols referenced:
  - PG_CMD_PUTS
  - PG_CMD_PRINTF  
  - escape_quotes
- Called from (representative examples):
  - initialize_data_directory (around line 3102)

## Notes and Other Information
- This is a static function within initdb.c, used specifically during database cluster initialization
- The pg_authid table contains password hashes and authentication information for all database roles
- Revoking public access to pg_authid is essential for security - passwords should only be accessible through system views with appropriate access controls
- The superuser password setting is optional and only occurs if superuser_password global variable is set
- Password escaping is performed using escape_quotes to handle special characters and prevent SQL injection
- This function is called during the final phases of database initialization after the basic catalog structure is in place
- The commands are executed as part of the bootstrap process where the backend is running in a special initialization mode