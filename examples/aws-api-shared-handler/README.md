# Shared Lambda handlers

This example registers 60 HTTP routes, 60 REST methods, and three WebSocket routes against one function. `addHandler` creates one API-scoped Lambda permission per API, rather than one permission per route. Reuse the returned handler; calling `addHandler` with another name creates another grant.

Run `sst deploy --stage <stage>`, then request `<http>/items-0` and `<rest>/items-59`. Both return `shared handler`. Connecting to the WebSocket URL invokes the same function through `$connect`.

Inspect the function's resource policy with `aws lambda get-policy --function-name <function>`. There should be three API Gateway statements. The permission remains if its routes are removed; remove the `addHandler` call when the target is no longer needed. Route order has no effect on the permission's name or owner.

For a versioned function, pass `fn.targetArn` (or an explicit alias/version ARN). Create separate bindings for different targets or APIs. Route authentication, throttling and integrations can still be configured independently.

For existing deployments, add the shared handler first and deploy, then switch the routes and deploy again. At the resource-policy size limit, first remove a redundant grant through the infrastructure that owns it, keeping another grant that permits the API to invoke the function. Roll back in the reverse order: restore per-route permissions before removing the shared handler. Do not try to restore more statements than fit the policy limit.
