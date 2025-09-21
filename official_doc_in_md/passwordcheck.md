F.24. passwordcheck — verify password strength  
---  
[Prev](pageinspect.md "F.23. pageinspect — low-level inspection of database pages") | [Up](contrib.md "Appendix F. Additional Supplied Modules and Extensions")| Appendix F. Additional Supplied Modules and Extensions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](pgbuffercache.md "F.25. pg_buffercache — inspect PostgreSQL
    buffer cache state")  
  
* * *

## F.24. passwordcheck — verify password strength #

The `passwordcheck` module checks users' passwords whenever they are set with [CREATE ROLE](sql-createrole.md "CREATE ROLE") or [ALTER ROLE](sql-alterrole.md "ALTER ROLE"). If a password is considered too weak, it will be rejected and the command will terminate with an error. 

To enable this module, add `'$libdir/passwordcheck'` to [shared_preload_libraries](runtime-config-client.md#GUC-SHARED-PRELOAD-LIBRARIES) in `postgresql.conf`, then restart the server. 

You can adapt this module to your needs by changing the source code. For example, you can use [CrackLib](https://github.com/cracklib/cracklib) to check passwords — this only requires uncommenting two lines in the `Makefile` and rebuilding the module. (We cannot include CrackLib by default for license reasons.) Without CrackLib, the module enforces a few simple rules for password strength, which you can modify or extend as you see fit. 

### Caution

To prevent unencrypted passwords from being sent across the network, written to the server log or otherwise stolen by a database administrator, PostgreSQL allows the user to supply pre-encrypted passwords. Many client programs make use of this functionality and encrypt the password before sending it to the server. 

This limits the usefulness of the `passwordcheck` module, because in that case it can only try to guess the password. For this reason, `passwordcheck` is not recommended if your security requirements are high. It is more secure to use an external authentication method such as GSSAPI (see [Chapter 20](client-authentication.md "Chapter 20. Client Authentication")) than to rely on passwords within the database. 

Alternatively, you could modify `passwordcheck` to reject pre-encrypted passwords, but forcing users to set their passwords in clear text carries its own security risks. 

* * *

[Prev](pageinspect.md "F.23. pageinspect — low-level inspection of database pages") | [Up](contrib.md "Appendix F. Additional Supplied Modules and Extensions")|  [Next](pgbuffercache.md "F.25. pg_buffercache — inspect PostgreSQL
    buffer cache state")  
---|---|---  
F.23. pageinspect — low-level inspection of database pages | [Home](index.md "PostgreSQL 17.5 Documentation")|  F.25. pg_buffercache — inspect PostgreSQL buffer cache state
