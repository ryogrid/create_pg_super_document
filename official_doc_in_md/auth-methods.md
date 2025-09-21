20.3. Authentication Methods  
---  
[Prev](auth-username-maps.md "20.2. User Name Maps") | [Up](client-authentication.md "Chapter 20. Client Authentication")| Chapter 20. Client Authentication| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](auth-trust.md "20.4. Trust Authentication")  
  
* * *

## 20.3. Authentication Methods #

PostgreSQL provides various methods for authenticating users: 

  * [Trust authentication](auth-trust.md "20.4. Trust Authentication"), which simply trusts that users are who they say they are. 

  * [Password authentication](auth-password.md "20.5. Password Authentication"), which requires that users send a password. 

  * [GSSAPI authentication](gssapi-auth.md "20.6. GSSAPI Authentication"), which relies on a GSSAPI-compatible security library. Typically this is used to access an authentication server such as a Kerberos or Microsoft Active Directory server. 

  * [SSPI authentication](sspi-auth.md "20.7. SSPI Authentication"), which uses a Windows-specific protocol similar to GSSAPI. 

  * [Ident authentication](auth-ident.md "20.8. Ident Authentication"), which relies on an “Identification Protocol” ([RFC 1413](https://datatracker.ietf.org/doc/html/rfc1413)) service on the client's machine. (On local Unix-socket connections, this is treated as peer authentication.) 

  * [Peer authentication](auth-peer.md "20.9. Peer Authentication"), which relies on operating system facilities to identify the process at the other end of a local connection. This is not supported for remote connections. 

  * [LDAP authentication](auth-ldap.md "20.10. LDAP Authentication"), which relies on an LDAP authentication server. 

  * [RADIUS authentication](auth-radius.md "20.11. RADIUS Authentication"), which relies on a RADIUS authentication server. 

  * [Certificate authentication](auth-cert.md "20.12. Certificate Authentication"), which requires an SSL connection and authenticates users by checking the SSL certificate they send. 

  * [PAM authentication](auth-pam.md "20.13. PAM Authentication"), which relies on a PAM (Pluggable Authentication Modules) library. 

  * [BSD authentication](auth-bsd.md "20.14. BSD Authentication"), which relies on the BSD Authentication framework (currently available only on OpenBSD). 




Peer authentication is usually recommendable for local connections, though trust authentication might be sufficient in some circumstances. Password authentication is the easiest choice for remote connections. All the other options require some kind of external security infrastructure (usually an authentication server or a certificate authority for issuing SSL certificates), or are platform-specific. 

The following sections describe each of these authentication methods in more detail. 

* * *

[Prev](auth-username-maps.md "20.2. User Name Maps") | [Up](client-authentication.md "Chapter 20. Client Authentication")|  [Next](auth-trust.md "20.4. Trust Authentication")  
---|---|---  
20.2. User Name Maps | [Home](index.md "PostgreSQL 17.5 Documentation")|  20.4. Trust Authentication
