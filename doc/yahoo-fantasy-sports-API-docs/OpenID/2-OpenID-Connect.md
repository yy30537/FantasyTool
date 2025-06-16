# OpenID Connect

[OpenID Connect](http://openid.net/connect/) is an [authentication](#concepts-authentication) mechanism built on top of OAuth 2.0. Web, mobile, and JavaScript Clients can use OpenID Connect to verify the identity and obtain basic profile information of users. This document is intended for developers creating applications that use OpenID Connect; thus, “you” will refer to the OAuth 2.0 role [client](http://tools.ietf.org/html/rfc6749#section-1.1).

Yahoo’s OAuth 2.0 APIs can be used for both authentication and authorization. In this document, we will focus on our OAuth 2.0 implementation for authentication, which conforms to the [OpenID Connect specification](http://openid.net/developers/specs/).

For your reference, we also suggest reading the [OpenID Connect FAQ](http://openid.net/connect/faq/) and the [OpenID Connect Discovery specification](http://openid.net/specs/openid-connect-discovery-1_0.html).

**Note**: The naming and syntax convention for terminology used in this documentation is based on the OpenID Connect Core specification.

## Benefits

OpenID Connect is a public-key-encryption-based authentication framework that offers the following:

- Globally increased Internet security by having the most expert service providers be responsible for verifying user identity.
- An easy, reliable, and secure method for developers to authenticate users across websites and applications while eliminating the need to store and manage user passwords.
- An easier way for users to sign up and register, which can reduce the [abandonment rate](https://en.wikipedia.org/wiki/Abandonment_rate) of your website or application.

## Key Concepts

Before starting, take a few minutes to digest the key concepts listed below. For a comprehensive list of terminology, see [OpenID Connect Core 1.0 Terminology](http://openid.net/specs/openid-connect-core-1_0.html#Terminology).

### Access Token

[Access Tokens](http://tools.ietf.org/html/rfc6749#section-1.4) are credentials for accessing protected resources. The Access Token is issued to you (the Client) after the Resource Owner (i.e., user) has authorized your site or application to access his or her protected data.

### Authentication

Authentication is the process of verifying the identity of a user signing in to a site or an application. In OpenID Connect, the authentication is decentralized, so the user’s information is not mapped to a private database. Thus, Yahoo as the [Identity Provider](#concepts-idp) allows [Relying Parties or Clients](#concepts-rp) to use OpenID Connect to authenticate their users through our service. As a result, users can use the same Yahoo credentials on multiple websites that support the OpenID Connect specification.

### Authorization

Authorization, instead of verifying identity, is the process whereby users grant applications access to their private data or account information. For example, an application, in addition to authenticating a user’s identity through Yahoo’s OpenID Connect, may also ask a user for permission to access private data from one or more Yahoo Web services. If the user grants permission, the application then receives an [Access Token](#concepts-access-token) that authorizes access to the user’s private data from the Yahoo Web service(s).

It should be noted that OAuth, and not OpenID Connect, is used to request an Access Token. You can, however, use the [Authorization Code Flow](#openid-connect-flows-auth-code) to get an [ID Token](#concepts-id-token) (OpenID Connect), Access Token (OAuth), and [Refresh Token](#concepts-refresh-token) (OAuth).

### Authorization Code

To request an Access Token or ID Token, you need to obtain authorization from the [Resource Owner](#concepts-resource-owner), such as the user. The authorization is expressed in the form of an authorization grant, which you use to request the tokens.

Authorization code is one such authorization grant which OAuth defines. It can be exchanged by you for an Access Token, Refresh Token, and/or ID Token from the token endpoint.

### Authorization Endpoint

The authorization endpoint is the URI used to obtain an authorization grant from a Resource Owner. The Resource Owner is the user who owns the data that your application wants to access. See [Authorization Endpoint](http://tools.ietf.org/html/rfc6749#section-3.1) in [RFC 6749](http://tools.ietf.org/html/rfc6749) for more details.

Yahoo’s authorization endpoint is `https://api.login.yahoo.com/oauth2/request_auth/`.

### ID Token

The ID Token is the primary extension that OpenID Connect makes to OAuth 2.0 to authenticate users. The [ID Token](http://openid.net/specs/openid-connect-core-1_0.html#IDToken) is a security token containing Claims (information) about the authentication of a user performed by Yahoo, and potentially other Claims requested by you. The ID Token is represented as a [JSON Web Token (JWT)](#concepts-jwt).

### Identity Provider (IDP)

The IDP is a party that offers user authentication as a service. In this document, Yahoo is the IDP. For more information, see “What do ‘IDP’ and ‘RP’ stand for?” and “Who can be an IDP?” in the [OpenID Connect FAQ and Q&As](http://openid.net/connect/faq/).

### JSON Web Token (JWT)

[JWT](https://tools.ietf.org/html/draft-ietf-oauth-json-web-token-32) is a compact, URL-safe means of representing and transferring claims between two parties.

### Refresh Token

[Refresh Tokens](https://tools.ietf.org/html/rfc6749#section-1.5) are credentials used to obtain Access Tokens when Access Tokens become invalid or expire. You receive an Access Token, a Refresh Token, and optionally, an ID Token from Yahoo in the Authorization Code Flow or in the Hybrid Flow.

### Relying Party (RP) / Client

The RP or Client is an application that outsources its user authentication function to an IDP. For example, you, as a developer, might want to use Yahoo as the IDP to authenticate the users of your site or application: this would make you the RP.

### Resource Owner

The Resource Owner is an entity (person or thing) capable of granting access to a protected resource. A Resource Owner that is a person is referred to as an *End-User*. We’ll be referring to End-Users simply as *users* in the document because this document is intended for developers.

### Single-Sign On (SSO)

SSO allows users to sign in once and access multiple related sites/applications. OpenID Connect makes SSO possible. In this document, users sign in at Yahoo for SSO.

### Token Endpoint

You present an authorization code or Refresh Token to the token endpoint to obtain an Access Token and/or ID Token. See [Token Endpoint](http://tools.ietf.org/html/rfc6749#section-3.2) in [RFC 6749](http://tools.ietf.org/html/rfc6749) for more information.

Yahoo’s token endpoint is `https://api.login.yahoo.com/oauth2/get_token`.

## Supported Authentication Flows

We support the following two OpenID Connect flows:

### Authorization Code Flow

In this flow, an Authorization Code is returned to you and can then be exchanged for an ID Token, an Access Token, and a Refresh Token. In this exchange, no tokens are exposed to the User Agent or possibly malicious applications with access to the User Agent. The Refresh Token can be used to refresh the Access Token for long-term access to a user’s private data.

The Authorization Code Flow is suitable for applications that can securely maintain a Client Secret between themselves and the [Authorization Server](http://openid.net/specs/openid-connect-core-1_0.html#Authenticates) (Yahoo). For a breakdown of the authentication steps, see [Authorization Code Flow Steps](http://openid.net/specs/openid-connect-core-1_0.html#CodeFlowSteps) in the OpenID Connect specification.

**Note**: The Authorization Code Flow in OpenID Connect is the same as the [Authorization Code Grant Flow](https://developer.yahoo.com/oauth2/guide/flows_authcode/) in OAuth 2.0.

#### When to Use

The Authorization Code Flow, in addition to allowing SSO, also lets you (the Client) get long-term access to user’s data through the Access Token and the Refresh Token.

### Hybrid Flow

When using the Hybrid Flow, some tokens are returned from the Authorization Endpoint and others are returned from the Token Endpoint. See the [Hybrid Flow Steps](http://openid.net/specs/openid-connect-core-1_0.html#HybridFlowSteps) in the Open Connect Core 1.0 specification.

#### When to Use

The [Hybrid Flow](http://openid.net/specs/openid-connect-core-1_0.html#HybridFlowAuth) supports the application receiving some tokens in the URL response as well as the ‘code’ as defined in the Authorization Code Grant flow. It allows you (the Client) to request either the ID Token or Access Token or both, along with the authorization code from the authorization endpoint. The code, thus obtained, can then be exchanged for the remaining tokens from the token endpoint.

For example, suppose you are primarily interested in using SSO. You can first obtain the code and the ID Token from the authorization endpoint. While the ID Token is being decoded to sign in the user, you can simultaneously exchange the code for the Access Token and Refresh Token from the token endpoint in a non-blocking manner.

Although the Hybrid Flow offers more flexibility with the ability to request and use tokens, it is less secure than the Authorization Code Flow because the tokens are exposed to the User Agent.