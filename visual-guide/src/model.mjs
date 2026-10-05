export const chapters = [
  { id: "index", label: "The big picture", group: "Start outside", title: "State, with a place to run.", subtitle: "A visual field guide to StateX: a WebAssembly actor, a private SQLite database, and an object store that makes both portable.", depth: 0 },
  { id: "architecture", label: "Anatomy of the system", group: "Peel the layers", title: "Small pieces. One protocol.", subtitle: "Follow the boundaries between the API, node, runtime, database, page log, and shared store. Each component has one very specific job.", depth: 1 },
  { id: "journey", label: "Life of a request", group: "Peel the layers", title: "A return is not yet a reply.", subtitle: "Walk a call from any node to the owner, through a transaction, and across the durability boundary before the client hears success.", depth: 2 },
  { id: "leases", label: "Leases & ownership", group: "Peel the layers", title: "One actor. One current owner.", subtitle: "Node leases answer who is alive. Actor records answer who owns what. Sessions and epochs prevent a restarted process from impersonating its past.", depth: 3 },
  { id: "durability", label: "Durability & recovery", group: "Peel the layers", title: "The disk is a cache.", subtitle: "The recoverable state lives in snapshots and page segments. A new owner reconstructs SQLite, then starts a self-contained epoch.", depth: 4 },
  { id: "runtime", label: "Inside the runtime", group: "Peel the layers", title: "A sandbox with useful doors.", subtitle: "WIT defines the API. Wasmtime runs the component. Explicit host imports connect guest code to SQL, HTTP, identity, logging, and other actors.", depth: 5 },
  { id: "failures", label: "When things fail", group: "Understand the edges", title: "Failure is part of the API.", subtitle: "Separate rollback from uncertain completion, and node liveness from ownership. The safest retry starts with knowing what a response cannot tell you.", depth: 5 },
  { id: "development", label: "From code to cluster", group: "Understand the edges", title: "Your interface is the route.", subtitle: "Author a WIT contract, implement it, test it, and publish a component with migrations. Nodes discover deployments; actors adopt them lazily.", depth: 2 },
  { id: "use-cases", label: "Where StateX fits", group: "Build on the pattern", title: "Choose the right unit of state.", subtitle: "A good actor key draws a transaction boundary. Explore real repository examples and application designs that benefit from isolated, durable state.", depth: 0 },
  { id: "patterns", label: "Agents, queues & cron", group: "Build on the pattern", title: "Different systems. Same core.", subtitle: "Persist a state machine, serialize each transition, and make retries safe. Add an external driver where the engine does not supply one.", depth: 1 },
  { id: "reference", label: "Field notes & sources", group: "Keep nearby", title: "The details worth keeping.", subtitle: "Defaults, API shapes, glossary, source map, and the assumptions behind the illustrations. Grounded in this working tree, not a generic actor framework.", depth: 5 }
];

export const layers = [
  { name: "Application", short: "A durable object per key", text: "Choose an app, an exported actor type, and a key. counter(\"alice\") and counter(\"bob\") have independent databases, even though they run the same code.", target: "use-cases", color: "#cce9de" },
  { name: "Routing", short: "Any node is a front door", text: "The receiving node looks for a local resident actor, consults owner.json when needed, and forwards to a live owner or competes to activate the actor.", target: "architecture", color: "#acd8c7" },
  { name: "Ownership", short: "A lease plus an epoch", text: "A node lease carries a process session and expiry. An actor owner record carries that session and a monotonic epoch. Compare-and-swap decides who may activate.", target: "leases", color: "#7fbda5" },
  { name: "Transaction", short: "One call, one SQLite boundary", text: "The host starts BEGIN IMMEDIATE, applies pending migrations, and invokes the component. A successful guest result commits; a method error or trap rolls back.", target: "journey", color: "#4b957b" },
  { name: "Durability", short: "Pages before acknowledgement", text: "Committed WAL pages become a transaction segment in the object store. Before acknowledging that write, the node checks its lease and current ownership.", target: "durability", color: "#245e4d" }
];

export function leasePhase(time, ttl = 10) {
  const margin = Math.max(ttl / 5, 0.2);
  return { margin, cutoff: ttl - margin, phase: time < ttl - margin ? "valid" : time < ttl ? "gap" : "takeover" };
}

export function restorePlan(entries) {
  const epochs = [...new Set(entries.map(e => e.epoch))].sort((a, b) => b - a);
  for (const epoch of epochs) {
    const snapshots = entries.filter(e => e.epoch === epoch && e.kind === "snapshot").map(e => e.txid);
    if (!snapshots.length) continue;
    const snapshot = Math.max(...snapshots);
    const candidates = entries.filter(e => e.epoch === epoch && e.kind === "segment" && e.txid > snapshot).map(e => e.txid).sort((a, b) => a - b);
    const segments = [];
    let next = snapshot + 1;
    for (const txid of candidates) {
      if (txid !== next) break;
      segments.push(txid);
      next += 1;
    }
    return { epoch, snapshot, segments, txid: next - 1 };
  }
  return null;
}

export const crashPoints = [
  { label: "Before commit", title: "No committed database change", tone: "green", result: "The transaction is rolled back or lost with the local process.", recovery: "Restore the previous durable state. No new segment for this call exists.", retry: "This illustrated crash has no committed SQL effect. A real client disconnect alone does not establish that this was the crash point; external HTTP or a callee may already have acted." },
  { label: "After local commit", title: "Local commit is not durability", tone: "amber", result: "SQLite changed, but this scenario crashes before any segment upload.", recovery: "Recovery sees the old durable state, not the lost local commit.", retry: "Do not infer this case from a 503. A store request that errors can still have applied; use an idempotency key." },
  { label: "After upload", title: "Durable, but not acknowledged", tone: "amber", result: "The segment reached the store, then the process crashed before replying.", recovery: "A later restore may include the write. The client has an unknown outcome.", retry: "Blindly repeating increment can increment twice. Record the request key and its result in the actor transaction." },
  { label: "After reply", title: "An acknowledged write survives", tone: "green", result: "The segment is uploaded and the ownership checks passed before the successful reply.", recovery: "Under the documented lease and store assumptions, the next owner restores the acknowledged state.", retry: "If the client received the reply, it knows the operation succeeded. If the reply was lost in transit, the client still needs deduplication." }
];

export const errors = [
  ["200", "success", "The result is in {\"result\": ...}. A method-level result::ok is unwrapped.", "Read the result."],
  ["201", "created", "Explicit _create succeeded.", "The actor exists; migrations were touched."],
  ["400", "bad_request", "Malformed JSON, invalid actor key, or arguments that do not match WIT.", "Fix the request, not the retry count."],
  ["401", "unauthorized", "Internal invoke has an invalid HMAC or a timestamp more than 60 seconds away.", "Fix peer authentication or clock alignment."],
  ["404", "not_found", "Unknown route, app, type, method, or deletion of a nonexistent/deleted actor.", "Check deployment and addressing."],
  ["405", "method_not_allowed", "A recognized route was called with the wrong HTTP verb.", "Use the documented verb."],
  ["409", "conflict", "_create encountered an existing, non-deleted actor.", "Call it, or choose a different key."],
  ["422", "method_error", "Guest returned result::err(E); E is in error.detail. The local transaction rolls back.", "Handle the domain error."],
  ["500", "trap / internal", "Guest trap rolls back SQL; internal execution/storage bookkeeping errors have a broader scope.", "Inspect the code and logs. Do not assume every internal 500 means no effect."],
  ["503", "unavailable", "Lease, routing, activation, upload, or ownership could not be established or confirmed.", "Unknown write outcome. Retry with application-level idempotency."],
  ["508", "cycle", "Target already on the actor call chain, or chain length reached 16.", "Remove re-entry or split the protocol into separate steps."]
];
