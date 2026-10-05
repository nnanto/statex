import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { chapters } from "./src/model.mjs";

const port = Number(process.env.PORT || 4317);
if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error("PORT must be an integer from 1 to 65535");
const files = new Set(chapters.map(c => `${c.id}.html`));
const directory = new URL("./dist/", import.meta.url);
const server = createServer(async (req, res) => {
  if (!["GET", "HEAD"].includes(req.method)) {
    res.writeHead(405, { "content-type": "text/plain", allow: "GET, HEAD" });
    res.end("Method not allowed");
    return;
  }
  const pathname = new URL(req.url, "http://localhost").pathname;
  const file = pathname === "/" ? "index.html" : pathname.slice(1);
  if (!files.has(file)) {
    res.writeHead(404, { "content-type": "text/plain" });
    res.end("Chapter not found");
    return;
  }
  try {
    const html = await readFile(new URL(file, directory));
    res.writeHead(200, { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" });
    res.end(req.method === "HEAD" ? undefined : html);
  } catch (error) {
    console.error(`Unable to serve ${file}: ${error.message}`);
    res.writeHead(500, { "content-type": "text/plain" });
    res.end("Guide build is missing or unreadable. Run npm run build in visual-guide.");
  }
});
server.on("error", error => {
  console.error(`Cannot start the guide server: ${error.message}`);
  process.exitCode = 1;
});
server.listen(port, "127.0.0.1", () => console.log(`StateX guide: http://127.0.0.1:${port}\nServing ${fileURLToPath(directory)} (loopback only)`));
