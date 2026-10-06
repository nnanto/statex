# StateX visual field guide

Open **[`dist/index.html`](dist/index.html)** in a browser. Each of the 11 chapters is a separate, self-contained HTML file with React, diagrams, and CSS embedded. The guide works offline, including from `file://`; it makes no API calls and loads no CDN scripts, fonts, analytics, or images.

## Chapters

1. **The big picture** — an interactive onion model and cluster topology.
2. **Anatomy of the system** — eight component contracts, node layout, and state boundaries.
3. **Life of a request** — an eight-step write/read/error explorer and the acknowledgement gate.
4. **Leases & ownership** — adjustable lease timeline, CAS race, sessions, epochs, and fencing.
5. **Durability & recovery** — WAL page capture, restore planner with gap/epoch experiments, streaming snapshots, and compaction.
6. **Inside the runtime** — Wasmtime sandbox, WIT/JSON mapping, host imports, and typed actor calls.
7. **When things fail** — crash-window explorer, searchable failure matrix, errors, and idempotency.
8. **From code to cluster** — authoring, testing, deployment, migrations, and code generation.
9. **Where StateX fits** — repository examples and clearly labeled application design ideas.
10. **Agents, queues & cron** — agent, queue, cron, and workflow state-machine designs with explicit external-driver requirements.
11. **Field notes & sources** — exact defaults, API routes, searchable glossary, source references, assumptions, and non-guarantees.

The labs illustrate the **default SQLite/WAL backend**, not requirements for
all extensions. The framework also accepts custom state backends, object
stores, host capabilities and transports; see `docs/extensions/`. Workspaces
and namespaced application names are optional. Source references appear
alongside explanations. The interactive labs are illustrative models, not
live cluster simulators or formal proofs. Agent/queue/cron/workflow designs
are **not built-in StateX features**.

## Rebuild and maintain

From this separate folder:

```sh
npm ci --ignore-scripts
npm run build
npm test
npm start
```

The optional preview server listens only on `http://127.0.0.1:4317`. Override its port with `PORT=4318 npm start`. It serves only generated chapter files; it does not expose the repository or execute StateX.

Node.js 16.19+ works with the pinned dependencies. React and ReactDOM power the UI; esbuild creates the embedded bundle; jsdom runs local interaction tests. Bundled React, ReactDOM, and scheduler license notices are embedded in each generated HTML file. Install scripts are not needed. No plugins, hooks, MCP integrations, or remotely fetched scripts are used at runtime.

Edit `src/pages.jsx` for chapter content, `src/interactives.jsx` for diagrams/labs, `src/model.mjs` for chapter metadata and deterministic models, and `src/styles.css` for presentation. Run the build after edits: `dist/` is deliberately retained so the result is usable without installing dependencies.

Navigation uses real relative HTML links. Mobile chapter navigation, keyboard-accessible controls, reduced-motion support, labeled SVGs, responsive layouts, and per-chapter printing are included. Printing captures the currently selected state of interactive examples; expand any detail sections you want to print.

The tests cover every generated page, offline dependencies, local navigation, source-grounded lease/restore model cases, component selection, every request branch, CAS reset/winners, recovery switches, failure filters, use-case categories, all four system patterns, glossary search, chapter search, and mobile menu state. Browser layout is additionally inspectable through the optional preview.
