# Discovery Document

The OpenID Connect protocol requires the use of multiple endpoints for authenticating users and for requesting resources including tokens, user information, and public keys.

To simplify implementations and increase flexibility, OpenID Connect allows the use of a Discovery document. This document contains JSON with key-value pairs providing details about the OpenID Connect provider’s configuration, authorization URIs, tokens, user information, and endpoints for public keys.


# OpenID Connect Discovery 1.0 incorporating errata set 2

## Abstract

OpenID Connect 1.0 is a simple identity layer on top of the OAuth 2.0 protocol. It enables Clients to verify the identity of the End-User based on the authentication performed by an Authorization Server, as well as to obtain basic profile information about the End-User in an interoperable and REST-like manner.

This specification defines a mechanism for an OpenID Connect Relying Party to discover the End-User's OpenID Provider and obtain information needed to interact with it, including its OAuth 2.0 endpoint locations.

## Table of Contents

1. [Introduction](#introduction)
   1.1. [Requirements Notation and Conventions](#requirements-notation-and-conventions)
   1.2. [Terminology](#terminology)
2. [OpenID Provider Issuer Discovery](#openid-provider-issuer-discovery)
   2.1. [Identifier Normalization](#identifier-normalization)
      2.1.1. [User Input Identifier Types](#user-input-identifier-types)
      2.1.2. [Normalization Steps](#normalization-steps)
   2.2. [Non-Normative Examples](#non-normative-examples)
      2.2.1. [User Input using E-Mail Address Syntax](#user-input-using-e-mail-address-syntax)
      2.2.2. [User Input using URL Syntax](#user-input-using-url-syntax)
      2.2.3. [User Input using Hostname and Port Syntax](#user-input-using-hostname-and-port-syntax)
      2.2.4. [User Input using "acct" URI Syntax](#user-input-using-acct-uri-syntax)
3. [OpenID Provider Metadata](#openid-provider-metadata)
4. [Obtaining OpenID Provider Configuration Information](#obtaining-openid-provider-configuration-information)
   4.1. [OpenID Provider Configuration Request](#openid-provider-configuration-request)
   4.2. [OpenID Provider Configuration Response](#openid-provider-configuration-response)
   4.3. [OpenID Provider Configuration Validation](#openid-provider-configuration-validation)
5. [String Operations](#string-operations)
6. [Implementation Considerations](#implementation-considerations)
   6.1. [Compatibility Notes](#compatibility-notes)
7. [Security Considerations](#security-considerations)
   7.1. [TLS Requirements](#tls-requirements)
   7.2. [Impersonation Attacks](#impersonation-attacks)
8. [IANA Considerations](#iana-considerations)
   8.1. [Well-Known URI Registry](#well-known-uri-registry)
      8.1.1. [Registry Contents](#registry-contents)
   8.2. [OAuth Authorization Server Metadata Registry](#oauth-authorization-server-metadata-registry)
      8.2.1. [Registry Contents](#registry-contents)
9. [References](#references)
   9.1. [Normative References](#normative-references)
   9.2. [Informative References](#informative-references)
Appendix A. [Acknowledgements](#acknowledgements)
Appendix B. [Notices](#notices)

## 1. Introduction

OpenID Connect 1.0 is a simple identity layer on top of the OAuth 2.0 protocol. It enables Clients to verify the identity of the End-User based on the authentication performed by an Authorization Server, as well as to obtain basic profile information about the End-User in an interoperable and REST-like manner.

In order for an OpenID Connect Relying Party to utilize OpenID Connect services for an End-User, the RP needs to know where the OpenID Provider is. OpenID Connect uses WebFinger to locate the OpenID Provider for an End-User. This process is described in Section 2.

Once the OpenID Provider has been identified, the configuration information for that OP is retrieved from a well-known location as a JSON document, including its OAuth 2.0 endpoint locations. This process is described in Section 4.

### 1.1. Requirements Notation and Conventions

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in RFC 2119.

### 1.2. Terminology

This specification uses the terms "Authorization Code", "Authorization Endpoint", "Authorization Server", "Client", "Client Authentication", "Client Secret", "Grant Type", "Response Type", and "Token Endpoint" defined by OAuth 2.0, the terms "Claim Name", "Claim Value", and "JSON Web Token (JWT)" defined by JSON Web Token (JWT), and the terms defined by OpenID Connect Core 1.0 and OAuth 2.0 Multiple Response Type Encoding Practices.

This specification also defines the following terms:

- **Resource**: Entity that is the target of a request in WebFinger.
- **Host**: Server where a WebFinger service is hosted.
- **Identifier**: Value that uniquely characterizes an Entity in a specific context.

## 2. OpenID Provider Issuer Discovery

OpenID Provider Issuer discovery is the process of determining the location of the OpenID Provider.

Issuer discovery is OPTIONAL; if a Relying Party knows the OP's Issuer location through an out-of-band mechanism, it can skip this step and proceed to Section 4.

The following information is needed to perform issuer discovery using WebFinger:

- **resource**: Identifier for the target End-User that is the subject of the discovery request.
- **host**: Server where a WebFinger service is hosted.
- **rel**: URI identifying the type of service whose location is being requested.

OpenID Connect uses the following discoverable `rel` value in WebFinger:

| Rel Type               | URI                                           |
|------------------------|-----------------------------------------------|
| OpenID Connect Issuer  | http://openid.net/specs/connect/1.0/issuer   |

### 2.1. Identifier Normalization

The purpose of Identifier normalization is to determine normalized Resource and Host values from the user input Identifier. These are then used as WebFinger request parameters to discover the Issuer location.

#### 2.1.1. User Input Identifier Types

A user input Identifier can be categorized into the following types, which require different normalization processes:

1. User input Identifiers starting with the XRI global context symbols ('=','@', and '!') are RESERVED. Processing of these identifiers is out of scope for this specification.
2. All other user input Identifiers MUST be treated as a URI in one of the forms `scheme "://" authority path-abempty [ "?" query ] [ "#" fragment ]` or `authority path-abempty [ "?" query ] [ "#" fragment ]` or `scheme ":" path-rootless`.

#### 2.1.2. Normalization Steps

A string of any other type is interpreted as a URI in one of the forms `scheme "://" authority path-abempty [ "?" query ] [ "#" fragment ]` or `authority path-abempty [ "?" query ] [ "#" fragment ]` or `scheme ":" path-rootless` and is normalized according to the following rules:

1. If the user input Identifier does not have an RFC 3986 scheme component, the string is interpreted as `[userinfo "@"] host [":" port] path-abempty [ "?" query ] [ "#" fragment ]`.
2. If the userinfo and host components are present and all of the scheme, path, query, port, and fragment components are absent, the `acct` scheme is assumed.
3. For all other inputs without a scheme component, the `https` scheme is assumed.
4. When the input contains an explicit scheme such as `acct` or `https` that matches the RFC 3986 `scheme ":" path-rootless` syntax, no input normalization is performed.
5. If the resulting URI contains a fragment component, it MUST be stripped off, together with the fragment delimiter character "#".

### 2.2. Non-Normative Examples

#### 2.2.1. User Input using E-Mail Address Syntax

To find the Issuer for the given user input in the form of an e-mail address `joe@example.com`, the WebFinger parameters are as follows:

| WebFinger Parameter | Value                                      |
|---------------------|--------------------------------------------|
| resource            | acct:joe@example.com                      |
| host                | example.com                               |
| rel                 | http://openid.net/specs/connect/1.0/issuer|

#### 2.2.2. User Input using URL Syntax

To find the Issuer for the given URL, `https://example.com/joe`, the WebFinger parameters are as follows:

| WebFinger Parameter | Value                                      |
|---------------------|--------------------------------------------|
| resource            | https://example.com/joe                   |
| host                | example.com                               |
| rel                 | http://openid.net/specs/connect/1.0/issuer|

#### 2.2.3. User Input using Hostname and Port Syntax

If the user input is in the form of `host:port`, e.g., `example.com:8080`, then it is assumed as the authority component of the URL.

#### 2.2.4. User Input using "acct" URI Syntax

To find the Issuer for the given user input in the form of an account URI `acct:juliet%40capulet.example@shopping.example.com`, the WebFinger parameters are as follows:

| WebFinger Parameter | Value                                      |
|---------------------|--------------------------------------------|
| resource            | acct:juliet%40capulet.example@shopping.example.com |
| host                | shopping.example.com                       |
| rel                 | http://openid.net/specs/connect/1.0/issuer|

## 3. OpenID Provider Metadata

OpenID Providers have metadata describing their configuration. These OpenID Provider Metadata values are used by OpenID Connect:

- **issuer**: REQUIRED. URL using the `https` scheme with no query or fragment components that the OP asserts as its Issuer Identifier.
- **authorization_endpoint**: REQUIRED. URL of the OP's OAuth 2.0 Authorization Endpoint.
- **token_endpoint**: URL of the OP's OAuth 2.0 Token Endpoint.
- **userinfo_endpoint**: RECOMMENDED. URL of the OP's UserInfo Endpoint.
- **jwks_uri**: REQUIRED. URL of the OP's JWK Set document.
- **registration_endpoint**: RECOMMENDED. URL of the OP's Dynamic Client Registration Endpoint.
- **scopes_supported**: RECOMMENDED. JSON array containing a list of the OAuth 2.0 scope values that this server supports.
- **response_types_supported**: REQUIRED. JSON array containing a list of the OAuth 2.0 `response_type` values that this OP supports.
- **response_modes_supported**: OPTIONAL. JSON array containing a list of the OAuth 2.0 `response_mode` values that this OP supports.
- **grant_types_supported**: OPTIONAL. JSON array containing a list of the OAuth 2.0 Grant Type values that this OP supports.
- **acr_values_supported**: OPTIONAL. JSON array containing a list of the Authentication Context Class References that this OP supports.
- **subject_types_supported**: REQUIRED. JSON array containing a list of the Subject Identifier types that this OP supports.
- **id_token_signing_alg_values_supported**: REQUIRED. JSON array containing a list of the JWS signing algorithms supported by the OP for the ID Token.
- **id_token_encryption_alg_values_supported**: OPTIONAL. JSON array containing a list of the JWE encryption algorithms supported by the OP for the ID Token.
- **id_token_encryption_enc_values_supported**: OPTIONAL. JSON array containing a list of the JWE encryption algorithms supported by the OP for the ID Token.
- **userinfo_signing_alg_values_supported**: OPTIONAL. JSON array containing a list of the JWS signing algorithms supported by the UserInfo Endpoint.
- **userinfo_encryption_alg_values_supported**: OPTIONAL. JSON array containing a list of the JWE encryption algorithms supported by the UserInfo Endpoint.
- **userinfo_encryption_enc_values_supported**: OPTIONAL. JSON array containing a list of the JWE encryption algorithms supported by the UserInfo Endpoint.
- **request_object_signing_alg_values_supported**: OPTIONAL. JSON array containing a list of the JWS signing algorithms supported by the OP for Request Objects.
- **request_object_encryption_alg_values_supported**: OPTIONAL. JSON array containing a list of the JWE encryption algorithms supported by the OP for Request Objects.
- **request_object_encryption_enc_values_supported**: OPTIONAL. JSON array containing a list of the JWE encryption algorithms supported by the OP for Request Objects.
- **token_endpoint_auth_methods_supported**: OPTIONAL. JSON array containing a list of Client Authentication methods supported by this Token Endpoint.
- **token_endpoint_auth_signing_alg_values_supported**: OPTIONAL. JSON array containing a list of the JWS signing algorithms supported by the Token Endpoint for the signature on the JWT used to authenticate the Client at the Token Endpoint.
- **display_values_supported**: OPTIONAL. JSON array containing a list of the `display` parameter values that the OpenID Provider supports.
- **claim_types_supported**: OPTIONAL. JSON array containing a list of the Claim Types that the OpenID Provider supports.
- **claims_supported**: RECOMMENDED. JSON array containing a list of the Claim Names of the Claims that the OpenID Provider MAY be able to supply values for.
- **service_documentation**: OPTIONAL. URL of a page containing human-readable information that developers might want or need to know when using the OpenID Provider.
- **claims_locales_supported**: OPTIONAL. Languages and scripts supported for values in Claims being returned, represented as a JSON array of BCP47 language tag values.
- **ui_locales_supported**: OPTIONAL. Languages and scripts supported for the user interface, represented as a JSON array of BCP47 language tag values.
- **claims_parameter_supported**: OPTIONAL. Boolean value specifying whether the OP supports use of the `claims` parameter.
- **request_parameter_supported**: OPTIONAL. Boolean value specifying whether the OP supports use of the `request` parameter.
- **request_uri_parameter_supported**: OPTIONAL. Boolean value specifying whether the OP supports use of the `request_uri` parameter.
- **require_request_uri_registration**: OPTIONAL. Boolean value specifying whether the OP requires any `request_uri` values used to be pre-registered.
- **op_policy_uri**: OPTIONAL. URL that the OpenID Provider provides to the person registering the Client to read about the OP's requirements on how the Relying Party can use the data provided by the OP.
- **op_tos_uri**: OPTIONAL. URL that the OpenID Provider provides to the person registering the Client to read about the OpenID Provider's terms of service.

## 4. Obtaining OpenID Provider Configuration Information

Using the Issuer location discovered as described in Section 2 or by other means, the OpenID Provider's configuration information can be retrieved.

OpenID Providers supporting Discovery MUST make a JSON document available at the path formed by concatenating the string `/.well-known/openid-configuration` to the Issuer.

### 4.1. OpenID Provider Configuration Request

An OpenID Provider's configuration information MUST be retrieved using an HTTP `GET` request at the previously specified path.

### 4.2. OpenID Provider Configuration Response

The response is a set of Claims about the OpenID Provider's configuration, including all necessary endpoints and public key location information.

### 4.3. OpenID Provider Configuration Validation

If any of the validation procedures defined in this specification fail, any operations requiring the information that failed to correctly validate MUST be aborted and the information that failed to validate MUST NOT be used.

## 5. String Operations

Processing some OpenID Connect messages requires comparing values in the messages to known values. Comparisons between JSON strings and other Unicode strings MUST be performed as specified below:

1. Remove any JSON applied escaping to produce an array of Unicode code points.
2. Unicode Normalization MUST NOT be applied at any point to either the JSON string or to the string it is to be compared against.
3. Comparisons between the two strings MUST be performed as a Unicode code point to code point equality comparison.

## 6. Implementation Considerations

This specification defines features used by both Relying Parties and OpenID Providers that choose to implement Discovery. All of these Relying Parties and OpenID Providers MUST implement the features that are listed in this specification as being "REQUIRED" or are described with a "MUST".

### 6.1. Compatibility Notes

Potential compatibility issues that were previously described in the original version of this specification have since been addressed.

## 7. Security Considerations

### 7.1. TLS Requirements

Implementations MUST support TLS. Which version(s) ought to be implemented will vary over time and depend on the widespread deployment and known security vulnerabilities at the time of implementation.

### 7.2. Impersonation Attacks

TLS certificate checking MUST be performed by the RP, as described in Section 7.1, when making an OpenID Provider Configuration Request.

## 8. IANA Considerations

### 8.1. Well-Known URI Registry

This specification registers the well-known URI defined in Section 4 in the IANA "Well-Known URIs" registry.

#### 8.1.1. Registry Contents

- URI suffix: `openid-configuration`
- Change controller: OpenID Foundation Artifact Binding Working Group - openid-specs-ab@lists.openid.net
- Specification document: Section 4 of this specification
- Related information: (none)

### 8.2. OAuth Authorization Server Metadata Registry

This specification registers the following metadata names in the IANA "OAuth Authorization Server Metadata" registry.

#### 8.2.1. Registry Contents

- Metadata Name: `userinfo_endpoint`
- Metadata Description: URL of the OP's UserInfo Endpoint
- Change Controller: OpenID Foundation Artifact Binding Working Group - openid-specs-ab@lists.openid.net
- Specification Document(s): Section 3 of this specification


# Yahoo’s OpenID Connect configuration

Yahoo Discovery document

```json

{
    "issuer": "https://api.login.yahoo.com",
    "authorization_endpoint": "https://api.login.yahoo.com/oauth2/request_auth",
    "token_endpoint": "https://api.login.yahoo.com/oauth2/get_token",
    "introspection_endpoint": "https://api.login.yahoo.com/oauth2/introspect",
    "userinfo_endpoint": "https://api.login.yahoo.com/openid/v1/userinfo",
    "token_revocation_endpoint": "https://api.login.yahoo.com/oauth2/revoke",
    "jwks_uri": "https://api.login.yahoo.com/openid/v1/certs",
    "response_types_supported": [
      "code",
      "token",
      "id_token",
      "code token",
      "code id_token",
      "token id_token",
      "code token id_token"
    ],
    "subject_types_supported": [
      "public"
    ],
    "grant_types_supported": [
      "authorization_code",
      "refresh_token"
    ],
    "id_token_signing_alg_values_supported": [
      "ES256",
      "RS256"
    ],
    "scopes_supported": [
      "openid",
      "openid2",
      "profile",
      "email"
    ],
    "acr_values_supported": [
      "AAL1",
      "AAL2"
    ],
    "token_endpoint_auth_methods_supported": [
      "client_secret_basic",
      "client_secret_post"
    ],
    "claims_supported": [
      "aud",
      "email",
      "email_verified",
      "birthdate",
      "exp",
      "family_name",
      "given_name",
      "iat",
      "iss",
      "locale",
      "name",
      "sub",
      "auth_time"
    ],
    "response_modes_supported": [
      "query"
    ],
    "display_values_supported": [
      "page"
    ],
    "claims_parameter_supported": false,
    "request_parameter_supported": false,
    "request_uri_parameter_supported": false
  }

```