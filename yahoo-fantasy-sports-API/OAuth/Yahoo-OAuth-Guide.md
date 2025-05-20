# Yahoo OAuth 2.0 Guide

OAuth 2.0 is an updated version of the OAuth protocol that supercedes OAuth 1.0 and 1.0a. OAuth is an open standard for authorization that Yahoo uses to grant access to user data.

Note

OAuth 2.0 is currently supported by Oath Ad Platforms and UserInfo APIs.

## Benefits

OAuth 2.0 has some key distinctions from OAuth 1.0:
- SSL for secure communication.
- Signatures are no longer necessary.
- Support for a variety of grant types and flows.

## Supported Client Profiles

Yahoo supports two primary client profiles:
- Server-side Application: This consists of an application (client) hosted on a web server. Users access the application using an HTML based user agent. Client credentials and tokens issued are stored on the web server and are inaccessible to the user.

- Client-side Application: In this profile, the client code is downloaded from a web server and runs within a user-agent on the user’s device. Credentials and tokens are accessible to the end user.

## Supported Authorization Flows

As per the OAuth 2.0 specification, authorization to access user (resource owner) data can be obtained using four grant types. Yahoo currently supports one of the four grant types:

- Authorization Code Grant: This grant type is used to obtain access tokens which can be used to authorize access to Yahoo APIs.

# OAuth 2.0 FAQ
This section covers common FAQs with OAuth 2.0 integrations along with possible solutions.

## Does changing my password invalidate the refresh token? What will happen when I change my password?
Yes, all your refresh tokens are revoked after you change your password.

## Will I be disconnected from all my authorized OAuth apps when I change my password?
Yes, you will be disconnected from all your authorized OAuth apps.

## Will I always receive the same refresh token when exchanging the refresh token with the access token?
Not always. The best practice is to store the latest refresh token as the refresh token may change.

## Is there any way to complete the authentication process without opening up the browser?
No, an user must log in to the browser


# OAuth 2.0 Troubleshooting

This section covers common issues with OAuth 2.0 integrations along with possible solutions.

## Issues with Error Codes

### 401 Invalid Grant

If you receive a “401 Invalid Grant error” `{"error":"invalid_grant"`, ensure that the callback area for your app on Yahoo Developer Network is empty. If the error persists, create a new app and leave the callback blank. That will allow you to bypass the `invalid_grant`.

### 401 Forbidden

If you are trying to retrieve an access token through a browser, you may get the following 401 Forbidden error: `Oops. Yahoo is unable to process your request. We recommend that you contact the owner of the application or web site to resolve this issue. [95022]`

Ensure that requests are made from code or cURL calls instead of through a browser. Refer to Step 4: Exchange authorization code for access token for details.

## Version Issues

Ensure that requests follow the OAuth 2.0 spec instead of the OAuth 1.0a that also exists.

## Issues with the Authorization Header

If you are having issues with the authorization header, first ensure that the client ID and secret are encoded correctly using the following format: base64(clientid:clientsecret)

There is no newline at the end of client secret.

## Issues with Application Type

If you plan to manage your own data with a stand-alone app, ensure that you get a consumer key and secret by specifying “Installed Application”, not “Web Application”.

## Issues with Mispelled Code
Ensure that you correctly spell all request parameters. For example, do not spell `redirect_uri` as `redirect_url`.