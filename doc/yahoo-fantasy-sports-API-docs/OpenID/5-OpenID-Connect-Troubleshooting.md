# Troubleshooting

## Authorization Endpoint Errors

When you send an authentication request to Yahoo, if the request has missing or invalid parameters, we will display error messages intended for the user and the developer.

The screenshot below shows the two error messages with labels:

- ![number_one](https://s.yimg.com/oo/cms/products/oauth2/openid_connect/images/number_one_88b502d40.png) This error message is intended for the user and only states that something went wrong.
- ![number_two](https://s.yimg.com/oo/cms/products/oauth2/openid_connect/images/number_two_5ab0ed35b.png) This grayed-out message is intended for the developer and gives the cause of the error.

![Screenshot of auth errors](https://s.yimg.com/oo/cms/products/oauth2/openid_connect/images/auth_endpoint_errors_ae065ed51.jpg)

## User Denies Access

### Using Redirect URI

If the user denies access to his or her data by clicking **Not Now**, Yahoo will redirect to your `redirect_uri`, but instead of getting an authorization code or ID Token, you will get the query string parameter `error=access_denied`. Your application should programmatically handle the case where users deny access to their private data and act accordingly.

### No Redirect URI

If you specified `oob` for the `redirect_uri`, when a user clicks **Not Now**, Yahoo will attempt to close the **Yahoo Consent** dialog box.

## Token Endpoint Errors

If your request to the token endpoint fails, the returned JSON will contain an `error` field providing a succinct cause of the error and an `error_description` field providing details of the error.

In the example error response below, the `error` field indicates that the request was invalid, and the `error_description` field explains that the `refresh_token` parameter cannot be empty.

```json
{
    "error_description": "refresh token parameter cannot be empty for refresh_token grant type",
    "error": "invalid_request"
}
```

## Error Codes

The table below lists the possible error codes and descriptions.

| Code | Description |
|------|-------------|
| `ERROR_HANDLING_REQUEST` | Error handling request. This is the equivalent of an Internal Server Error. Make sure that you are not making a GET request when only POST is supported for an endpoint. |
| `INVALID_INPUT` | The value for `client_id` cannot be empty. |
| `INVALID_INPUT` | The value for `client_secret` cannot be empty. |
| `INVALID_INPUT` | Grant type cannot be null. |
| `INVALID_INPUT` | Redirect URL cannot be empty for `authorization_code` grant type. |
| `INVALID_INPUT` | The `code` parameter cannot be empty for authorization_code grant type. |
| `INVALID_AUTHORIZATION_CODE` | OAuth authorization code expired or invalid. |
| `SERVER_ERROR` | Internal error while processing the request. |
| `SESSION_VERIFICATION_FAIL` | Session verification fail. |

**Note:** The error codes and messages above are subject to change and may not be complete.