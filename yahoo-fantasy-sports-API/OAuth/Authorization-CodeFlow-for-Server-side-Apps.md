# Authorization Code Flow for Server-side Apps

You should use this flow when you have a server-side (Web) application.

![Authorization Code Flow for Server-side Apps](https://s.yimg.com/oo/cms/products/oauth2/flows_authcode/images/yahoo_auth_flow_04974dd18.png)

## Step 1: Sign in and get credentials

First, get a Consumer Key and Consumer Secret by signing in at [developer.yahoo.com](https://developer.yahoo.com/apps/create/) and creating a project. You will use these credentials for later calls in the OAuth 2.0 flow.

To create an app project, Yahoo needs information about your application including:

- Name
- Type
- Home page URL
- Scopes (permissions for specific services)
- Application domain

## Step 2: Get an authorization URL and authorize access

Use the Consumer Key we provide as the `client_id` to request a redirect URL. Also include the `redirect_url` so that Yahoo knows where to redirect users after they authorize access to their data.

**URL**: `https://api.login.yahoo.com/oauth2/request_auth`

**Method**: `GET`, `POST`

Table 1 Request Authorization URL (/request_auth) Call Request

| Request Parameters | Description |
|--------------------|-------------|
| `client_id`        | Consumer Key provided to you when you signed up. |
| `redirect_uri`     | Yahoo redirects Users to this URL after they authorize access to their private data. If the user should not be redirected to your server, you should specify the callback as `oob` (out of band). |
| `response_type`    | Must contain the string `code`. |
| `state`            | Optional. Your client can insert state information that will be appended to the `redirect_uri` upon successful user authorization. |
| `language`         | Optional. Language identifier. Default value is `en-us`. |

**Sample URL**

`https://api.login.yahoo.com/oauth2/request_auth?client_id=dj0yJmk9ak5IZ2x5WmNsaHp6JmQ9WVdrOVNqQkJUMnRYTjJrbWNHbzlNQS0tJnM9Y29uc3VtZXJzZWNyZXQmeD1hYQ--&redirect_uri=oob&response_type=code&language=en-us`

**Note**: In the authorization code flow, you will only need to reauthorize access from the user in the future if the user revokes access through [Yahoo account settings](https://help.yahoo.com/kb/yahoo-account/SLN2631.html?impressions=true).

## Step 3: User redirected for access authorization

A successful response to `request_auth` initiates a 302 redirect to Yahoo where the user can authorize access.

## Step 4: Exchange authorization code for access token

Once the user authorizes access, the user is redirected back to the `redirect_uri` you originally specified. An authorization code is appended to the `redirect_uri`, shown below as `code=abcdef`:

`https://www.example.com/callback?code=abcdef&state=XYZ`

Your client needs to extract the authorization code and exchange it for an access token using a call to the `/get_token` endpoint. The response also contains the refresh token, which persists even when the user changes passwords. The authorization server may issue a new refresh token, in which case the client must discard the old refresh token and replace it with the new refresh token. The authorization server will revoke the old refresh token after issuing a new refresh token to the client. The refresh token can also be invalidated if the user revokes access through [Yahoo account settings](https://help.yahoo.com/kb/yahoo-account/SLN2631.html?impressions=true).

**URL**: `https://api.login.yahoo.com/oauth2/get_token`

**Method**: `POST`

Table 2 Get Access Token (/get_token) Request Parameters

| Request Parameters | Description |
|--------------------|-------------|
| `client_id`        | Consumer Key provided to you when you signed up. |
| `client_secret`    | The Consumer Secret provided to you when you signed up. |
| `redirect_uri`     | Yahoo redirects Users to this URL after they authorize access to their private data. If your application does not have access to a browser, you must specify the callback as `oob` (out of band). |
| `code`             | Authorization code appended to `redirect_uri` in the previous call. |
| `grant_type`       | Must contain the string `authorization_code` grant type. |

**Sample Request Header**

```bash
Authorization: Basic ZGoweUptazlhazVJWjJ4NVdtTnNhSHA2Sm1ROVdWZHJPVk5xUWtKVU1uUllUakpyYldOSGJ6bE5RUzB0Sm5NOVkyOXVjM1Z0WlhKelpXTnlaWFFtZUQxaFlRLS06NmYzYjI5NjllYzUwOTkxNDM4MDdiNDU4ZTU5MTc5MzFmYmEzMWUwOA==
Content-Type: application/x-www-form-urlencoded
```

**Note**: The `Authorization: Basic` authorization header is generated through a Base64 encoding of `client_id:client_secret` per [RFC 2617](http://tools.ietf.org/html/rfc2617#section-2). You can use [https://www.base64encode.org/](https://www.base64encode.org/) to see how headers should be encoded.

**Sample Request Body**

```bash
grant_type=authorization_code&redirect_uri=https%3A%2F%2Fwww.example.com&code=abcdef
```

**Sample Response**

```json
{
   "access_token":"Jzxbkqqcvjqik2IMxGFEE1cuaos--",
   "token_type":"bearer",
   "expires_in":3600, 
   "refresh_token":"AOiRUlJn_qOmByVGTmUpwcMKW3XDcipToOoHx2wRoyLgJC_RFlA-",
   "xoauth_yahoo_guid":"JT4FACLQZI2OCE"
}
```

Table 3 /get_token call response

| Request Parameters | Description |
|--------------------|-------------|
| `access_token`     | The access token that you can use to make calls for Yahoo user data. The access token has a 1-hour lifetime. |
| `token_type`       | The access token that you can use to make calls for Yahoo user data. |
| `expires_in`       | The access token lifetime in seconds. |
| `refresh_token`    | The refresh token that you can use to acquire a new access token after the current one expires. |
| `xoauth_yahoo_guid`| The [GUID](https://developer.yahoo.com/social/rest_api_guide/ysocial_apis-guids.html) of the Yahoo user. (This claim is deprecated. If you need the user’s GUID value, please use the OpenID Connect flows. The GUID will be provided in the id_token.) |

**Important**: You should store the refresh token for future use. You will need to provide the refresh token to get a new access token when it expires.

## Step 5: Exchange refresh token for new access token

After the access token expires, you can use the refresh token, which has a long lifetime, to get a new access token.

**URL**: `https://api.login.yahoo.com/oauth2/get_token`

**Method**: `POST`

Table 4 Get Access Token (/get_token) Request Parameters

| Request Parameters | Description |
|--------------------|-------------|
| `client_id`        | Consumer Key provided to you when you signed up. |
| `client_secret`    | The Consumer Secret provided to you when you signed up. |
| `redirect_uri`     | Yahoo redirects Users to this URL after they authorize access to their private data. If your application does not have access to a browser, you must specify the callback as `oob` (out of band). |
| `refresh_token`    | The refresh token that you originally received along with the access token. |
| `grant_type`       | Must contain the `refresh_token` grant type. |

**Sample Request Header**

```bash
Authorization: Basic ZGoweUptazlhazVJWjJ4NVdtTnNhSHA2Sm1ROVdWZHJPVk5xUWtKVU1uUllUakpyYldOSGJ6bE5RUzB0Sm5NOVkyOXVjM1Z0WlhKelpXTnlaWFFtZUQxaFlRLS06NmYzYjI5NjllYzUwOTkxNDM4MDdiNDU4ZTU5MTc5MzFmYmEzMWUwOA==
Content-Type: application/x-www-form-urlencoded
```

**Sample Request Body**

```bash
grant_type=refresh_token&redirect_uri=https%3A%2F%2Fwww.example.com&refresh_token=a_qOmByVGTm
```

**Sample Response**

```json
{
   "access_token":"Jzxbkqqcvjqik2IMxGFEE1cuaos--",
   "token_type":"bearer",
   "expires_in":3600,
   "refresh_token":"AOiRUlJn_qOmByVGTmUpwcMKW3XDcipToOoHx2wRoyLgJC_RFlA-",
   "xoauth_yahoo_guid":"JT4FACLQZI2OCE"
}
```

Table 5 /get_token call response

| Request Parameters | Description |
|--------------------|-------------|
| `access_token`     | The access token that you can use to make calls for Yahoo user data. The access token has a 1-hour lifetime. |
| `token_type`       | The access token that you can use to make calls for Yahoo user data. |
| `expires_in`       | The access token lifetime in seconds. |
| `refresh_token`    | The refresh token that you can use to acquire a new access token after the current one expires. |
| `xoauth_yahoo_guid`| The [GUID](https://developer.yahoo.com/social/rest_api_guide/ysocial_apis-guids.html) of the Yahoo user. (This claim is deprecated. If you need the user’s GUID value, please use the OpenID Connect flows. The GUID will be provided in the id_token.) |

