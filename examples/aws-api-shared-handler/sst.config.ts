/// <reference path="./.sst/platform/config.d.ts" />

export default $config({
  app() {
    return { name: "aws-api-shared-handler", home: "aws", removal: "remove" };
  },
  async run() {
    const fn = new sst.aws.Function("Backend", { handler: "index.handler" });
    const http = new sst.aws.ApiGatewayV2("Http");
    const rest = new sst.aws.ApiGatewayV1("Rest");
    const ws = new sst.aws.ApiGatewayWebSocket("WebSocket");
    const httpHandler = http.addHandler("Backend", fn.arn);
    const restHandler = rest.addHandler("Backend", fn.arn);
    const wsHandler = ws.addHandler("Backend", fn.arn);
    for (let i = 0; i < 60; i++) {
      http.route(`GET /items-${i}`, httpHandler);
      rest.route(`GET /items-${i}`, restHandler);
    }
    rest.deploy();
    ws.route("$connect", wsHandler);
    ws.route("$disconnect", wsHandler);
    ws.route("$default", wsHandler);
    return { http: http.url, rest: rest.url, ws: ws.url, function: fn.name };
  },
});
