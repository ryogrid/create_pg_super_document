20.14. BSD Authentication  
---  
[Prev](auth-pam.md "20.13. PAM Authentication") | [Up](client-authentication.md "Chapter 20. Client Authentication")| Chapter 20. Client Authentication| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](client-authentication-problems.md "20.15. Authentication Problems")  
  
* * *

## 20.14. BSD Authentication #

This authentication method operates similarly to `password` except that it uses BSD Authentication to verify the password. BSD Authentication is used only to validate user name/password pairs. Therefore the user's role must already exist in the database before BSD Authentication can be used for authentication. The BSD Authentication framework is currently only available on OpenBSD. 

BSD Authentication in PostgreSQL uses the `auth-postgresql` login type and authenticates with the `postgresql` login class if that's defined in `login.conf`. By default that login class does not exist, and PostgreSQL will use the default login class. 

### Note

To use BSD Authentication, the PostgreSQL user account (that is, the operating system user running the server) must first be added to the `auth` group. The `auth` group exists by default on OpenBSD systems. 

* * *

[Prev](auth-pam.md "20.13. PAM Authentication") | [Up](client-authentication.md "Chapter 20. Client Authentication")|  [Next](client-authentication-problems.md "20.15. Authentication Problems")  
---|---|---  
20.13. PAM Authentication | [Home](index.md "PostgreSQL 17.5 Documentation")|  20.15. Authentication Problems
