20.9. Peer Authentication  
---  
[Prev](auth-ident.md "20.8. Ident Authentication") | [Up](client-authentication.md "Chapter 20. Client Authentication")| Chapter 20. Client Authentication| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](auth-ldap.md "20.10. LDAP Authentication")  
  
* * *

## 20.9. Peer Authentication #

The peer authentication method works by obtaining the client's operating system user name from the kernel and using it as the allowed database user name (with optional user name mapping). This method is only supported on local connections. 

The following configuration options are supported for `peer`: 

`map`
    

Allows for mapping between system and database user names. See [Section 20.2](auth-username-maps.md "20.2. User Name Maps") for details. 

Peer authentication is only available on operating systems providing the `getpeereid()` function, the `SO_PEERCRED` socket parameter, or similar mechanisms. Currently that includes Linux, most flavors of BSD including macOS, and Solaris. 

* * *

[Prev](auth-ident.md "20.8. Ident Authentication") | [Up](client-authentication.md "Chapter 20. Client Authentication")|  [Next](auth-ldap.md "20.10. LDAP Authentication")  
---|---|---  
20.8. Ident Authentication | [Home](index.md "PostgreSQL 17.5 Documentation")|  20.10. LDAP Authentication
