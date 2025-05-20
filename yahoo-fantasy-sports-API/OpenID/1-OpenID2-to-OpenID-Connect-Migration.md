# OpenID2 to OpenID Connect Migration

The basic techniques of using Yahoo OpenID Connect are described in [Getting Started with OpenID Connect](https://developer.yahoo.com/oauth2/guide/openid_connect/getting_started.html).

To migrate your current OpenID2.0 client to OpenID Connect, the following steps should be performed:

If you don’t have a client created before for your OpenID2 app, you should create one first. Check this for more details: [Setting Up - Create an Application and Get OAuth 2.0 Credentials](https://developer.yahoo.com/oauth2/guide/openid_connect/getting_started.html#i-setting-up-create-an-application-and-get-oauth-2-0-credentials).

When constructing the authorization request to `/request_auth` API:

- Add scopes to the `scope` parameter of your request by mapping the value `openid.ext1.required` to standard OpenID Connect scope.

| `openid.ext1.required` | OpenID Connect scope |
|------------------------|----------------------|
| `email`                | `email`              |
| Other scopes like `first`, `last`, `country`, etc. | `profile` |

The user information requested will be encoded in the ID token you get from the `/get_token` API. (Details about the content of an ID token can be found [here](https://developer.yahoo.com/oauth2/guide/openid_connect/decode_id_token.html).)

- Add `openid2` to the `scope` parameter, delimited by a space with other scopes you actually need.
- Add the `openid2_realm` parameter, using the same value as you put in the `openid.realm` parameter when calling the OpenID 2 API.
- Use the value of `openid.return_to` as the `redirect_uri`.

For example, if you are sending an OpenID 2 authorize request URL like:

```
https://open.login.yahooapis.com/openid/op/auth?....&openid.return_to=https%3A%2F%2Fmydomain.com%2Fopenid&openid.realm=https%3A%2F%2Fmydomain.com&openid.ext1.required=email%2Cfirst%2Clast%2Ccountry
```

Now you should construct your OpenID Connect authorize request URL as:

```
https://api.login.yahoo.com/oauth2/request_auth?client_id={your_client_id}&response_type=code&redirect_uri=https%3A%2F%2Fmydomain.com%2Fopenid&scope=openid%20email%20profile%20openid2&openid_realm=https%3A%2F%2Fmydomain.com&nonce=YihsFwGKgt3KJUh6tPs2.
```

When getting back the ID Token, you need to decode it and verify the `openid2_id` by making a GET call to the obtained verified claimed ID with an `Accept` header set to `application/json`. This API will return a JSON with `iss` as its top-level member. The value of the `iss` field must exactly match the `iss` in the ID Token.

For example, you get an ID token with OpenID 2 Identifier like this:

```json
{
  "iss": "https://api.login.yahoo.com",
  ...,
  "openid2_id": "https://me.yahoo.com/a/Xsg0e4k2zPyKEiPoBRIjNKfHdcG.gA--"
}
```

You should make a GET request to:

```
https://me.yahoo.com/a/Xsg0e4k2zPyKEiPoBRIjNKfHdcG.gA--
```

Request:

```
GET /a/Xsg0e4k2zPyKEiPoBRIjNKfHdcG.gA-- HTTP/1.1
Host: me.yahoo.com
Accept: application/json
```

Response:

```
HTTP/1.1 200 OK
Content-Type: application/json

{
  "iss": "https://api.login.yahoo.com"
}
```

You should verify the `iss` field in the response matches the `iss` in the ID token.
