import { build } from "esbuild";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { chapters } from "./src/model.mjs";

const root = fileURLToPath(new URL(".", import.meta.url));
const output = join(root, "dist");
const result = await build({
  entryPoints: [join(root, "src/app.jsx")],
  bundle: true,
  write: false,
  minify: true,
  format: "iife",
  target: ["es2020"],
  define: { "process.env.NODE_ENV": '"production"' },
  legalComments: "inline"
});
const script = result.outputFiles[0].text.replace(/<\/script/gi, "<\\/script");
const css = (await readFile(join(root, "src/styles.css"), "utf8")).replace(/<\/style/gi, "<\\/style");
const notices = (await Promise.all(["react", "react-dom", "scheduler"].map(async name =>
  `${name}\n${await readFile(join(root, "node_modules", name, "LICENSE"), "utf8")}`
))).join("\n\n");
const escape = value => value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;");
await mkdir(output, { recursive: true });
for (const chapter of chapters) {
  const html = `<!doctype html>
<html lang="en" data-page="${chapter.id}">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="description" content="${escape(chapter.subtitle)}">
  <meta name="theme-color" content="#142c26">
  <title>${escape(chapter.label)} | StateX visual field guide</title>
  <!-- Bundled third-party license notices:
${notices.replaceAll("--", "- -")}
  -->
  <style>${css}</style>
</head>
<body>
  <div id="root"></div>
  <noscript><main><h1>StateX visual field guide</h1><p>This offline React guide needs JavaScript to render its chapters and interactive diagrams. Enable JavaScript, then reload this local HTML file. No server, CDN, account, or network connection is required.</p></main></noscript>
  <script>${script}</script>
</body>
</html>
`;
  await writeFile(join(output, `${chapter.id}.html`), html);
}
console.log(`Built ${chapters.length} standalone HTML chapters in visual-guide/dist/ (React and CSS embedded; no network dependencies).`);
