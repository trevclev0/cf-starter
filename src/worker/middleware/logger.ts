import type { AppEnv } from "@worker/types";
import { createMiddleware } from "hono/factory";

const loggedEnvironments = ["development", "preview", "production"];

export const requestIdMiddleware = createMiddleware<AppEnv>(async (c, next) => {
  const requestId = c.req.header("x-request-id") ?? crypto.randomUUID();
  c.set("requestId", requestId);
  c.header("x-request-id", requestId);
  await next();
});

/**
 * One structured JSON line per request. Skipped in tests so integration
 * output stays readable.
 */
export const conditionalLogger = createMiddleware<AppEnv>(async (c, next) => {
  const start = performance.now();
  await next();
  const durationMs = performance.now() - start;
  if (loggedEnvironments.includes(c.env.ENVIRONMENT || "")) {
    console.log(
      JSON.stringify({
        ts: new Date().toISOString(),
        level: "info",
        method: c.req.method,
        path: c.req.path,
        status: c.res.status,
        durationMs: Math.round(durationMs * 100) / 100,
        requestId: c.get("requestId"),
      }),
    );
  }
});
