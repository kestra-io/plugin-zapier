# How to use the Zapier plugin

Trigger Zapier Zaps from Kestra flows via webhook.

## Tasks

`TriggerZap` sends an HTTP request to a Zapier Catch Hook URL. Set `url` (required, the full Zapier webhook URL). Optionally set `content` (a map of key-value pairs sent as the JSON body), `method` (default `POST`, also `PUT` or `GET`), `headers` (custom HTTP headers), `timeout` (default 30 seconds), and `allowFailed: true` to suppress errors on non-2xx responses (default `false`).

## Output

The output includes `status` (HTTP response code), `body` (raw response string), and `attemptId` (the `X-Attempt-Id` or `X-Request-Id` response header value).

## Connection

Store the webhook URL in [secrets](https://kestra.io/docs/concepts/secret) and set it on each task.
