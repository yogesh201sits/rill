import { Hono } from "hono";

import { streamRoutes } from "../routes/stream.routes";

const app = new Hono();

app.get("/health", (c) => {
  return c.json({
    status: "ok",
  });
});

app.route("/", streamRoutes);

export default app;