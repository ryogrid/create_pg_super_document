20.12. Certificate Authentication  
---  
[Prev](auth-radius.md "20.11. RADIUS Authentication") | [Up](client-authentication.md "Chapter 20. Client Authentication")| Chapter 20. Client Authentication| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](auth-pam.md "20.13. PAM Authentication")  
  
* * *

## 20.12. Certificate Authentication #

This authentication method uses SSL client certificates to perform authentication. It is therefore only available for SSL connections; see [Section 18.9.2](ssl-tcp.md#SSL-OPENSSL-CONFIG "18.9.2. OpenSSL Configuration") for SSL configuration instructions. When using this authentication method, the server will require that the client provide a valid, trusted certificate. No password prompt will be sent to the client. The `cn` (Common Name) attribute of the certificate will be compared to the requested database user name, and if they match the login will be allowed. User name mapping can be used to allow `cn` to be different from the database user name. 

The following configuration options are supported for SSL certificate authentication: 

`map`
    

Allows for mapping between system and database user names. See [Section 20.2](auth-username-maps.md "20.2. User Name Maps") for details. 

It is redundant to use the `clientcert` option with `cert` authentication because `cert` authentication is effectively `trust` authentication with `clientcert=verify-full`. 

* * *

[Prev](auth-radius.md "20.11. RADIUS Authentication") | [Up](client-authentication.md "Chapter 20. Client Authentication")|  [Next](auth-pam.md "20.13. PAM Authentication")  
---|---|---  
20.11. RADIUS Authentication | [Home](index.md "PostgreSQL 17.5 Documentation")|  20.13. PAM Authentication
