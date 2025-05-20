# Getting Started

This chapter provides step-by-step instructions for using Yahoo’s OpenID Connect and is divided into the following two sections:

1. [Setting Up: Create an Application and Get OAuth 2.0 Credentials](#i-setting-up-create-an-application-and-get-oauth-20-credentials)
2. [Authorization Code Flow: Authenticating Users](#ii-authorization-code-flow-authenticating-users)

## I. Setting Up: Create an Application and Get OAuth 2.0 Credentials

1. You’ll need to [create a Yahoo account](https://edit.yahoo.com/registration) to set up applications on the [Yahoo Developer Network (YDN)](http://developer.yahoo.com).
2. After you have a Yahoo account, [create an application](https://developer.yahoo.com/apps/create/) to get your Client ID (Consumer Key) and Client Secret (Consumer Secret) for later use in the OpenID Connect / OAuth 2.0 flows.
3. In the **Create Application** form, provide an application name and a callback domain. The callback domain is where Yahoo will send responses to your authentication request, so you’ll want to be the domain owner.
4. If your application needs to access private user data from Yahoo APIs, you’ll need to request permissions to Yahoo APIs in the checklist under **API Permissions**. For the purpose of demonstration in this getting started, check **Mail** and then select **Read**.
5. Finish creating your application by clicking **Create App**.

**Tip:** You can always go to [My Apps](https://developer.yahoo.com/apps/) to view your applications and OAuth credentials.

## II. Authorization Code Flow: Authenticating Users

The Authorization Code Flow allows you to get an authorization code that you can exchange for an ID Token (OpenID), an Access Token (OAuth), and a Refresh Token (OAuth).

### Step 1: Send an authentication request to Yahoo

After you’ve created your application, you’ll be given a Client ID (Consumer Key) and Client Secret (Consumer Secret). You’ll be using the Consumer Key as the `client_id` and the callback domain you provided as the `redirect_uri` to redirect users after they authorize access to their data.

To create an authorization request, you’ll need Yahoo’s OAuth 2.0 authorization endpoint, a supported HTTP method, and the request parameters given below. Be sure to use the request parameter `response_code=code` to receive an ID Token, an Access Token, and a Refresh Token.

**OAuth 2.0 Authorization Endpoint:** `https://api.login.yahoo.com/oauth2/request_auth`

**Supported HTTP Methods:** `GET`, `POST`

The request parameters below can either be transmitted in the request body using `POST` or as part of the query string with `GET`.

| Request Parameters | Description |
|--------------------|-------------|
| `client_id` | (Required) The Client ID (Consumer Key) provided to you when you created your application. |
| `redirect_uri` | (Required) Yahoo redirects users to this URL after they agree to use SSO and authorize access to their private data. Provide the complete URL including the HTTP/HTTPS protocol. If the user should not be redirected to your server, specify the callback as `oob` (out of band). |
| `response_type` | (Required) For the Authorization Code Flow, you must use `code`. |
| `scope` | (Required for OpenID Connect) To get an ID Token to authenticate a user, you are required to specify the scope identifier `openid`. For example: `scope=openid`. Additionally, to access private user data from the Yahoo APIs, include the relevant [API scope identifiers](#). The scopes can be delimited by a space or comma. In the example below, the scope identifier is specified for requesting the ID Token and an Access Token that provides read access to the Yahoo Mail API: `scope=openid mail-r` or `scope=openid,mail-r`. |
| `state` | (Recommended) Create a unique session token to maintain state between the request and the callback. By cryptographically binding the value of this parameter to a browser cookie, you can mitigate [cross-site request forgery (CSRF, XSRF)](https://yahoo.jiveon.com/docs/DOC-64841?et=watches.email.document_comment#comment-31363). |
| `language` | (Optional) Language identifier. The default value is `en-us`. |
| `nonce` | (Required) An arbitrary URL-safe string used to associate your client session with an ID Token and to mitigate replay attacks. The value is passed through unmodified from the Authentication Request to the ID Token. See [Nonce Notes](http://openid.net/specs/openid-connect-core-1_0.html#NonceNotes) for more information. |
| `prompt` | (Optional) A string value specifying whether Yahoo prompts the user for reauthentication or consent. To prompt the user to re-authorize your application, include `prompt=consent` in the request. The **Yahoo Consent Screen** will then be displayed every time the user signs in to your application. To prompt the user to re-authenticate to Yahoo, include `prompt=login` in the request. This parameter can be used to make sure that the user is still present for the current session or to bring attention to the request. |
| `max_age` | (Optional) You can specify the allowable elapsed time in seconds since the last time the user was actively authenticated by Yahoo. If the elapsed time is greater than this value, Yahoo will attempt to actively re-authenticate the user. |

#### Sample URL

```http
https://api.login.yahoo.com/oauth2/request_auth?client_id=dj0yJmk9WGx0QlE0UWdCa0hKJmQ9WVdrOWNrNUhXVnBhTkhFbWNHbzlNQS0tJnM9Y29uc3VtZXJzZWNyZXQmeD01OA--&response_type=code&redirect_uri=https://yahoo.com&scope=openid%20mail-r&nonce=YihsFwGKgt3KJUh6tPs2
```

### Step 2: Obtain user consent

When a user attempts to sign in to your application and you send Yahoo an authentication request for SSO, Yahoo will first authenticate the user and then present the user with the **Yahoo Consent Screen**. From the **Yahoo Consent Screen**, users are able to view the permissions you are requesting and either agree or disagree to grant those permissions.

**Note:** No developer action is required in this step.

#### Try It

Now that you have formed your OpenID Connect authentication request, copy and paste it into your Web browser’s address bar. You’ll be redirected to the same **Yahoo Consent Screen**. Click **Agree** to go to your redirect URL and see your authorization code, which should look similar to the one shown below:

```http
https://yahoo.com?code=x2tzunc
```

### Step 3: Exchange authorization code for tokens

Once the user authorizes access, the user is redirected back to URL you assigned to the parameter `redirect_uri`. The authorization code that you’ll exchange for tokens (Access Token, ID Token, Refresh Token) is returned as a query string.

In the example below, the authorization code `code=x2tzunc` is returned as a query string parameter:

```http
https://yahoo.com/?code=x2tzunc
```

Your application needs to make a POST call to the `get_token` endpoint with the extracted authorization code and the request parameters in the table below.

**OAuth 2.0 Token Endpoint:** `https://api.login.yahoo.com/oauth2/get_token`

**Supported HTTP Methods:** `POST`

The request parameters below are transmitted using HTTP POST in the request body. You can, however, also send the parameters `client_id` and `client_secret` in the [HTTP Headers](#get-auth-tokens-step-3-http-header) instead.

| Request Parameters | Description |
|--------------------|-------------|
| `client_id` | (Required) The Client ID (Consumer Key) provided to you when you created your application. |
| `client_secret` | (Required) The Client Secret (Consumer Secret) provided to you when you created your application. |
| `redirect_uri` | (Required) Provide the same complete URL (including the HTTP/HTTPS protocol) given when requesting the authorization code or the value `oob` if `oob` was passed to obtain the authorization code. The `redirect_uri` is used solely as a security check as specified in [RFC 6749](http://www.rfc-editor.org/rfc/rfc6749.txt). |
| `code` | (Required) The authorization code appended to `redirect_uri` after calling Yahoo’s OAuth 2.0 authorization endpoint. |
| `grant_type` | (Required) Must contain the string `authorization_code` grant type. |

#### Sending Parameters in the HTTP Header and Request Body

When using [HTTP Basic authentication](https://en.wikipedia.org/wiki/Basic_access_authentication), encode the string `client_id:client_secret` with [Base64](https://en.wikipedia.org/wiki/Base64) scheme per [RFC 2617](http://tools.ietf.org/html/rfc2617#section-2). You can use [https://www.base64encode.org/](https://www.base64encode.org/) to encode the authorization header.

**Header**

```http
Content-Type: application/x-www-form-urlencoded
Authorization:Basic ZGoweUptazlhVmhuVVd0d1REUm5abko2Sm1ROVdWZEthbHBIVm0xaFFTMHRKbk05WTI5dWMzVnRaWEp6WldOeVpYUW1lRDA1TWctLTowOWVkNDU2ZjkyODY4MjAwOWI1MTMyMzcyYTBiZWVkZTM5YzgyZmEz
```

**Request Body**

```http
grant_type=authorization_code&redirect_uri=https://yahoo.com&code=x2tzunc
```

#### Sending Parameters in the Request Body

When sending the parameters in the request body, you need to provide both the Client ID (`client_id`) and the Client Secret (`client_secret`) as shown below:

```http
code=x2tzunc&grant_type=authorization_code&client_id=dj2yJmk9aEdiU1VRODg5RUk0JmQ9WVdrOVpITTNZbXgyTjJjbWNHbzlNQS0tJnM9Y29uc3VtZXJzZWNyZXQmeD0zYg--&client_secret=b7dec9d002316dda9a83d4fccd5a95d1329d3b5a&redirect_uri=https://yahoo.com
```

#### Response Body

A successful response contains JSON with the following fields:

| Fields | Description |
|--------|-------------|
| `access_token` | The Access Token signed by Yahoo. |
| `id_token` | A JWT digitally signed by Yahoo that contains identity information about the user. |
| `expires_in` | The Access Token lifetime in seconds. |
| `token_type` | Identifies the type of token returned. At this time, this field always has the value Bearer. |
| `refresh_token` | The Refresh Token that you can use to acquire a new Access Token after the current one expires. For details on how, see [Refreshing an Access Token](https://tools.ietf.org/html/rfc6749#section-6) in [RFC 6749](https://tools.ietf.org/html/rfc6749). |
| `xoauth_yahoo_guid` | The [GUID](https://developer.yahoo.com/social/rest_api_guide/ysocial_apis-guids.html) of the Yahoo user. |

**Example Response**

```json
{
   "access_token":"UNQO1djO5xpaKm3_KbECBKB5mlFr6tSZTOLrrJCprtT1X1UFljpxiS5iSue8u_n8ah1WbL6sTNw3HPFHicyXDbTs7aSrbIe.rx9n9dzX7xZjx8dyF2Ap1a6J_nw4k56a5mCOuTd.ZFQENgGtHwM0DRFVeDNTAx_WzhqDGPCqhtsNICuuY30soFZGS11FTlUk7Gy0ISjxLRAjIZVtpojnY5p8XuT1qUtAheWqZegJ_7t.AP4o0J4xJ3_oocXeiSKEXaD3AijdBdViKPZI3Ow7yeHK8uX1weNfKoSP6eEpCviyj0YlRMIBSg4cRdGL6EsSggX6B5gzgcA9efDSpcwVhupY0RlUdi.AxJ1nT0frWmrYiwntpu1XP_5mIbOlb4wfrD_ZCRNY2Qby40RBt5iHERSJ89K1o69fw3Jd4C3hF14iJLHcDHmnYJSX651G9MlpGPWT99DRteCdhSm8URbZqfGPG8mZtLpmhfxr1umCoGEgocrfHpITMjOyEwvgmAhgjGKXugvdNTABn0AEQBetIVtJ80Ymbn6IMq_Qh10vyspVsVK69C9yTlwLtZhcvim5Nk_15JHd0GSj0Mj.X.FWTzUK1e3CNQjeJxdQ2Qk9BXDC4_DXW_Ot5LzYy5qRvRKT4gh54n5aBROxFdky0ELt1IgkLTRJ0idUCen87klP.0CLp1QTNXx99N6nM9c_HwWVKwhILUjzXaIrP0GVEMwlGIHqn2I91Z03irBgzrMB219lqUAuF27_OD4QnyQfICSW65n5hVo1e89xwN6VN3usRrhHmdDfd7nk3nzMyXdsOPzghA1huBCYyEGZ_kq9FzVFQ5QYDmJ0WqpmG1yXDEntYVvkB_i_jkbNPH4.R134ptwznCZSuQ--",
   "refresh_token":"AJj.Dlbt_e4XN85buQhFXj77sIB3lqBF3Bcqb2kwUEoYrBb0Pg--",
   "expires_in":3600,
   "token_type":"bearer",
   "xoauth_yahoo_guid":"UQIDWJNWVNQD4GXZ5NGMZUSTQ4",
   "id_token":"eyJhbGciOiJFUzI1NiIsImtpZCI6IjM0NjZkNTFmN2RkMGM3ODA1NjU2ODhjMTgzOTIxODE2YzQ1ODg5YWQifQ.eyJhdF9oYXNoIjoiYWM5YkR3ejVMWjl5UEVpdWtEcGdzdz09Iiwic3ViIjoiVVFJRFdKTldWTlFENEdYWjVOR01aVVNUUTQiLCJhdWQiOiJkajB5Sm1rOVdHeDBRbEUwVVdkQ2EwaEtKbVE5V1Zkck9XTnJOVWhYVm5CaFRraEZiV05IYnpsTlFTMHRKbk05WTI5dWMzVnRaWEp6WldOeVpYUW1lRDAxT0EtLSIsImlzcyI6Imh0dHBzOi8vbG9naW4ueWFob28uY29tIiwiZXhwIjoxNDQzODI3MTMwLCJub25jZSI6IjEyMzQ1IiwiaWF0IjoxNDQzODIzNTMwfQ.n7oEFi5028StcI41Hkh6lLYK4PmF7pT4AIXrQ_62nfDEZj2g0oYjSLFPJp4IqF6LefwcCQ9FHT5X9eC8A7peqw"
}
```

### Step 4: Use your tokens

In this flow, you receive JSON with an Access Token, a Refresh Token, and an ID Token (`access_token`, `id_token`, `refresh_token`). You also receive an `xoauth_yahoo_guid` parameter that contains a user identifier, which can be used to get user information from Yahoo Web Services.

To learn to use your tokens, see the following:

- [Decoding the ID Token](#)
- [Refresh Token: Exchange Refresh Token for new Access Token](https://developer.yahoo.com/oauth2/guide/flows_authcode/#step-5-exchange-refresh-token-for-new-access-token)
