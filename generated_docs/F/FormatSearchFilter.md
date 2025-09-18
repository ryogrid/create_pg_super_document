# FormatSearchFilter

## Location
src/backend/libpq/auth.c: 2415 - 2437

## Overview
Formats an LDAP search filter template by substituting username placeholders with the actual username for authentication.

## Definition


## Detailed Description
The `FormatSearchFilter` function creates a customized LDAP search filter by processing a template pattern and replacing all occurrences of the placeholder "$username" with the actual username. This function is essential for LDAP authentication where search filters need to be dynamically constructed based on the connecting user.

The function uses PostgreSQL's StringInfo utilities to efficiently build the output string. It scans through the input pattern character by character, and when it encounters the "$username" placeholder (defined as LPH_USERNAME), it substitutes it with the provided username. All other characters are copied unchanged.

This mechanism allows administrators to configure flexible LDAP search filters in pg_hba.conf that can accommodate various LDAP directory schemas and organizational structures.

## Parameters / Member Variables
- `pattern`: The template string containing "$username" placeholders to be substituted
- `user_name`: The actual username to substitute in place of "$username" placeholders

## Dependencies
- Functions called/Symbols referenced:
  - `initStringInfo`: Initialize the StringInfo buffer
  - `appendStringInfoString`: Append the username to the output
  - `appendStringInfoChar`: Append individual characters to the output
  - `strncmp`: Compare pattern against LPH_USERNAME placeholder
  - LPH_USERNAME: Constant defining the "$username" placeholder string
  - LPH_USERNAME_LEN: Length of the placeholder string
- Called from (representative examples):
  - `CheckLDAPAuth` at src/backend/libpq/auth.c:2554

## Notes and Other Information
- Returns a newly allocated C string that must be freed by the caller
- The LPH_USERNAME constant is defined as "$username" and LPH_USERNAME_LEN as its length
- Used when `ldapsearchfilter` is specified in pg_hba.conf for custom LDAP search filters
- Enables flexible LDAP authentication by allowing administrators to define custom search patterns
- Example usage: pattern "(|(uid=$username)(mail=$username@domain.com))" with user_name "john" becomes "(|(uid=john)(mail=john@domain.com))"