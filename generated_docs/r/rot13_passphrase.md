# rot13_passphrase

## Location
[src/test/modules/ssl_passphrase_callback/ssl_passphrase_func.c:67-83](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/ssl_passphrase_callback/ssl_passphrase_func.c#L67-L83)

## Overview
Transforms a password using ROT13 cipher as part of PostgreSQL's LDAP authentication testing module.

## Definition

```c
static int
rot13_passphrase(char *buf, int size, int rwflag, void *userdata)
```
## Detailed Description
This function applies the ROT13 cipher transformation to a given password string, creating a new copy with all alphabetic characters shifted by 13 positions in the alphabet. ROT13 is a simple letter substitution cipher that replaces each letter with the letter 13 positions after it in the alphabet (wrapping around from Z to A). This function is used in PostgreSQL's LDAP password testing module to demonstrate custom password transformation hooks in the authentication system. The function allocates new memory for the transformed password and preserves non-alphabetic characters unchanged.

## Parameters / Member Variables
- : Input password string to be transformed using ROT13 cipher

## Dependencies
- Functions called/Symbols referenced:
  - strlen (to get input password length)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - strlcpy (safe string copying)
- Called from (representative examples):
  - [_PG_init](../P/_PG_init.md) (via ldap_password_hook assignment in ldap_password_func module)
  - [set_rot13](../s/set_rot13.md) (via SSL_CTX_set_default_passwd_cb in ssl_passphrase_callback module)
  - LDAP authentication system (when ldap_password_hook is triggered)

## Notes and Other Information
- Located in src/test/modules/ldap_password_func/ldap_password_func.c:47-65
- Also referenced in src/test/modules/ssl_passphrase_callback/ssl_passphrase_func.c:63
- ROT13 provides no real cryptographic security - it's purely for testing purposes
- Used in two different test modules: LDAP password transformation and SSL passphrase callbacks
- Returns a newly allocated string that must be managed by the caller
- Preserves the original password unchanged, creating a transformed copy
- Part of PostgreSQL's authentication testing infrastructure