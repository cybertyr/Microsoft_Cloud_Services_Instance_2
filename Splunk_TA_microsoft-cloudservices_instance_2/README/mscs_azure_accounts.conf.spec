##
## SPDX-FileCopyrightText: 2021 Splunk LLC <sales@splunk.com>
## SPDX-License-Identifier: LicenseRef-Splunk-8-2021
##
##

[<account_stanza_name>]
client_id = <string> the client id which is automatically generated when registering with Azure AD
auth_type = <string> the authentication type. Supported values: client_secret, client_certificate. Default: client_secret
client_secret = <string> the password for client id. Required when auth_type is client_secret
change_client_secret = <integer> UI-only flag indicating that the client_secret should be updated on edit. Persisted as 0 or 1
certificate_private_key = <string> PEM bundle containing both the certificate and the private key. Required when auth_type is client_certificate. To convert a .pfx file, run: openssl pkcs12 -in cert.pfx -out bundle.pem -nodes
certificate_password = <string> the password for the private key. Optional, only needed if the private key is encrypted
change_certificate_credentials = <integer> UI-only flag indicating that the certificate_private_key and certificate_password should be updated on edit. Persisted as 0 or 1
tenant_id = <string> the UUID which point to the AD containing your application
account_class_type = <integer> should be 1 or 2. 1 for Azure Public Cloud, 2 for Azure Government Cloud
