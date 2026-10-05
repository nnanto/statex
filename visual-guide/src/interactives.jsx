import React, { useState } from "react";
import { layers, leasePhase, restorePlan, crashPoints } from "./model.mjs";
import { Icon, Note, Source, Flow, Badge, Code, ChapterLink } from "./ui.jsx";

export function Onion() {
  const [selected, setSelected] = useState(0);
  const layer = layers[selected];
  return <div className="onion-layout">
    <div className="onion-art">
      <svg viewBox="0 0 480 450" role="img" aria-labelledby="onion-title onion-desc">
        <title id="onion-title">Five layers of StateX</title><desc id="onion-desc">Concentric rings: application, routing, ownership, transaction, durability. Use the layer buttons to explore each one.</desc>
        <defs><pattern id="grid" width="24" height="24" patternUnits="userSpaceOnUse"><circle cx="1" cy="1" r="1" fill="#a6bcb2" opacity=".38"/></pattern></defs>
        <rect width="480" height="450" fill="url(#grid)"/>
        {layers.map((l, i) => <circle key={l.name} cx="240" cy="216" r={194 - i * 34} fill={l.color} stroke={selected === i ? "#132e28" : "#f4faf7"} strokeWidth={selected === i ? 3 : 2}/>)}
        <g fill="#fff" textAnchor="middle"><text x="240" y="212" fontSize="20" fontWeight="700">statex</text><text x="240" y="234" fontSize="11" letterSpacing="1.3">DURABLE CORE</text></g>
        <path d="M265 217h168" fill="none" stroke="#e7f2ed" strokeDasharray="4 5"/>
        <text x="240" y="437" textAnchor="middle" fill="#5e746c" fontSize="12">PEEL FROM THE OUTSIDE IN</text>
      </svg>
    </div>
    <div className="onion-content"><div className="eyebrow">Five layers, one mental model</div><div className="layer-buttons" aria-label="Explore architecture layers">{layers.map((l, i) => <button key={l.name} aria-pressed={selected === i} onClick={() => setSelected(i)}><span className="layer-dot" style={{ background: l.color }}/>{String(i + 1).padStart(2, "0")} {l.name}</button>)}</div><div className="layer-description" aria-live="polite"><h3>{layer.short}</h3><p>{layer.text}</p><ChapterLink id={layer.target}>Explore this layer</ChapterLink></div></div>
  </div>;
}

export function Topology() {
  const [mode, setMode] = useState("forward");
  return <div className="diagram-panel">
    <div className="panel-bar"><span className="mini-label">The cluster at a glance</span><div className="segmented" aria-label="Routing scenario">{[["forward", "Existing actor"], ["activate", "First touch"]].map(([id, label]) => <button key={id} aria-pressed={mode === id} onClick={() => setMode(id)}>{label}</button>)}</div></div>
    <svg className="topology-svg" viewBox="0 0 940 335" role="img" aria-label={mode === "forward" ? "Client calls node A, which forwards to owner node B. Node B runs Wasm and SQLite, then uploads pages to the shared store." : "Client calls node A. Node A wins ownership CAS, restores the actor, and becomes its owner."}>
      <defs><marker id="arrowhead" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto"><path d="M0 0 7 3 0 6" fill="#5b917e"/></marker></defs>
      <rect x="490" y="28" width="420" height="176" rx="20" fill="#edf5f0" stroke="#b4d3c2" strokeDasharray="5 5"/>
      <text x="510" y="51" className="svg-label">{mode === "forward" ? "NODE B / CURRENT OWNER" : "NODE A / NEW OWNER"}</text>
      <path d="M155 114h58M365 114h148M642 114h42M746 148v107M287 147v131h224" className="svg-line" markerEnd="url(#arrowhead)"/>
      <g><rect x="25" y="80" width="130" height="68" rx="12" fill="#fff" stroke="#d6dfda"/><text x="90" y="111" textAnchor="middle" className="svg-title">Client / SDK</text><text x="90" y="130" textAnchor="middle" className="svg-caption">counter("alice")</text></g>
      <g><rect x="215" y="80" width="150" height="68" rx="12" fill="#fff" stroke="#abc8b9"/><text x="290" y="110" textAnchor="middle" className="svg-title">Any node</text><text x="290" y="130" textAnchor="middle" className="svg-caption">Node A receives</text></g>
      <text x="437" y="94" textAnchor="middle" className="svg-caption">{mode === "forward" ? "signed forward" : "CAS + activate"}</text>
      <g><rect x="516" y="80" width="126" height="68" rx="12" fill="#173f34"/><text x="579" y="110" textAnchor="middle" fill="#fff" fontSize="16" fontWeight="600">Wasm</text><text x="579" y="131" textAnchor="middle" fill="#c2ded1" fontSize="11">actor instance</text></g>
      <g><rect x="686" y="80" width="185" height="68" rx="12" fill="#fff" stroke="#abc8b9"/><text x="778" y="110" textAnchor="middle" className="svg-title">Private SQLite</text><text x="778" y="130" textAnchor="middle" className="svg-caption">one actor, one database</text></g>
      <text x="769" y="230" className="svg-caption">pages before reply</text>
      <text x="301" y="252" className="svg-caption">owner records + node leases</text>
      <rect x="514" y="255" width="365" height="62" rx="12" fill="#e3e9fb" stroke="#bec9eb"/>
      <text x="696" y="282" textAnchor="middle" className="svg-title">Shared object store</text><text x="696" y="302" textAnchor="middle" className="svg-caption">coordination + deployments + recoverable state</text>
      <text x="37" y="304" className="svg-caption">No separate consensus service or routing table.</text>
    </svg>
    <div className="diagram-caption" aria-live="polite">{mode === "forward" ? "Ownership sticks to the current node. The front door is not necessarily the executor." : "First touch wins only after a successful conditional write. A CAS loser re-reads and follows the winner."}</div>
  </div>;
}

const steps = [
  ["Address", "Client", "POST names app, actor type, key, and method. JSON arguments follow the exported WIT signature.", "No state touched", "neutral"],
  ["Route & lock", "Node", "Check the node lease and route metadata. Take the actor slot lock. Use a local resident actor, forward to a live remote owner, or acquire and activate.", "One local slot per actor", "neutral"],
  ["Begin", "SQLite", "BEGIN IMMEDIATE starts the host-owned transaction. Pending migrations run here, tracked in _statex_migrations.", "SQL changes are tentative", "amber"],
  ["Invoke", "Wasmtime", "The resident component runs the method and uses host imports. Guest return values, traps, and method-level errors determine the next branch.", "External calls already have effects", "amber"],
  ["Commit", "SQLite", "A successful guest return commits SQLite. The runtime does not yet send a client reply.", "Local commit only", "amber"],
  ["Replicate", "Object store", "Capture newly committed WAL pages, assign txid + 1, encode a segment, and PUT it under the current epoch.", "Recoverable before response", "mint"],
  ["Check", "Node + store", "For a segment-producing call: evaluate lease.valid(), then read owner.json and match state, node, session, and epoch. Failure drops the resident actor and returns 503.", "Acknowledgement gate", "mint"],
  ["Reply", "Client", "Return 200 with {result: ...}. Explicit creation returns 201. Compaction may be scheduled while holding the actor lock for a stable snapshot.", "Successful write acknowledged", "green"]
];

export function RequestStepper() {
  const [step, setStep] = useState(0);
  const [branch, setBranch] = useState("write");
  const current = steps[step];
  const description = branch === "error" && step >= 4 ? ["Rollback", "SQL rolls back on result::err or trap. No committed segment is emitted for this transaction. A method error becomes 422; a guest trap becomes 500. External effects are not undone."] : branch === "read" && step >= 4 ? ["No new WAL pages", "A call that commits no new pages skips both segment upload and the post-upload ownership-check branch. The invocation-entry lease check still applies. A first read can write migrations, in which case it takes the write path."] : [current[0], current[2]];
  return <div className="interactive">
    <div className="panel-bar"><span className="mini-label">Request explorer / illustrative, not a live cluster</span><div className="segmented" aria-label="Request type">{[["write", "Write"], ["read", "Read-only"], ["error", "Method error"]].map(([id, title]) => <button key={id} aria-pressed={branch === id} onClick={() => setBranch(id)}>{title}</button>)}</div></div>
    <div className="step-track" aria-label="Request steps">{steps.map((s, i) => <button key={s[0]} onClick={() => setStep(i)} aria-pressed={i === step} className={i < step ? "past" : ""}><span>{i < step ? <Icon name="check" size={15}/> : i + 1}</span><small>{s[0]}</small></button>)}</div>
    <div className="step-body" aria-live="polite"><div className="step-number">{String(step + 1).padStart(2, "0")}</div><div><Badge tone={branch === "error" && step >= 4 ? "red" : current[4]}>{branch === "write" || step < 4 ? current[1] : branch === "read" ? "No segment path" : "Rollback path"}</Badge><h3>{description[0]}</h3><p>{description[1]}</p></div></div>
    <div className="step-actions"><button className="button secondary" disabled={step === 0} onClick={() => setStep(step - 1)}>Previous step</button><span>{step + 1} / {steps.length}</span><button className="button" disabled={step === steps.length - 1} onClick={() => setStep(step + 1)}>Next step <Icon name="arrow" size={16}/></button></div>
    <Source files="crates/node/src/node.rs:309-506; crates/node/src/actor.rs:146-198"/>
  </div>;
}

export function LeaseLab() {
  const [time, setTime] = useState(4);
  const [ttl, setTtl] = useState(10);
  const info = leasePhase(time, ttl);
  const status = { valid: ["Owner can pass its lease check", "The last successful renewal started at t=0. No more renewals succeed in this illustration."], gap: ["Owner must not acknowledge", "The local safety window is closed, but peers still see an unexpired lease. This intentional gap is not a new owner's permission to run."], takeover: ["A peer can compete for ownership", "The stored lease has expired. The next caller can attempt owner.json CAS; there is no automatic actor migration at this tick."] }[info.phase];
  return <div className="interactive lease-lab">
    <div className="panel-bar"><span className="mini-label">Lease timeline / renewal loss</span><label className="inline-label">TTL <select aria-label="Lease TTL" value={ttl} onChange={e => { setTtl(Number(e.target.value)); setTime(0); }}><option value="10">10 seconds (default)</option><option value="5">5 seconds</option><option value="1">1 second</option></select></label></div>
    <div className="lease-readout" aria-live="polite"><span>t = <b>{time.toFixed(1)}</b> s</span><Badge tone={info.phase === "valid" ? "green" : info.phase === "gap" ? "amber" : "purple"}>{info.phase === "valid" ? "LEASE VALID" : info.phase === "gap" ? "SAFETY GAP" : "TAKEOVER ELIGIBLE"}</Badge></div>
    <div className="timeline-legend"><span>0 / last renewal start</span><span>{info.cutoff.toFixed(1)}s / local cutoff</span><span>{ttl}s / peer expiry</span></div>
    <div className="lease-track"><div className="lease-zone valid" style={{ width: `${info.cutoff / (ttl * 1.2) * 100}%` }}>May acknowledge</div><div className="lease-zone margin" style={{ width: `${info.margin / (ttl * 1.2) * 100}%` }}>Margin</div><div className="lease-zone expired">CAS</div><div className="time-marker" style={{ left: `${time / (ttl * 1.2) * 100}%` }}/></div>
    <label className="range-label">Drag time to see who may act<input aria-label="Elapsed lease time" type="range" min="0" max={ttl * 1.2} step="0.1" value={time} onChange={e => setTime(Number(e.target.value))}/></label>
    <div className="lease-explanation" aria-live="polite"><h3>{status[0]}</h3><p>{status[1]}</p></div>
    <div className="metrics three"><div><strong>{(ttl / 3).toFixed(2)}s</strong><span>normal renewal interval</span></div><div><strong>{info.margin.toFixed(1)}s</strong><span>max(TTL / 5, 200 ms)</span></div><div><strong>{info.cutoff.toFixed(1)}s</strong><span>local validity cutoff</span></div></div>
    <Note title="A deadline rule, not a precisely timed process exit" tone="amber">valid() becomes false at the margin. The renewal loop signals fencing when it observes failure or expiry; scheduling and in-flight I/O affect when that happens. Clock skew must remain below the safety margin.</Note>
    <Source files="crates/node/src/lease.rs:87-177"/>
  </div>;
}

export function CasRace() {
  const [winner, setWinner] = useState(null);
  return <div className="cas-race">
    <div className="panel-bar"><span className="mini-label">Competing for an expired owner's record</span><button className="text-button" onClick={() => setWinner(null)}>Reset race</button></div>
    <Code title="owner.json / illustrative versions">{winner ? `{ "node": "${winner}", "session": "new-${winner}",\n  "epoch": 8, "state": "owned" }\nETag: v42` : `{ "node": "old-node", "session": "old-session",\n  "epoch": 7, "state": "owned" }\nETag: v41 / node lease expired`}</Code>
    <div className="grid two">{["A", "B"].map(node => <div className={`race-node ${winner === node ? "won" : ""}`} key={node}><Icon name="cube"/><h3>Node {node}</h3><p>Read v41. Propose epoch 8.<br/>PUT If-Match: v41</p><button className="button secondary" disabled={winner !== null} onClick={() => setWinner(node)}>Let {node} reach CAS first</button></div>)}</div>
    <div role="status" className="race-result">{winner ? `Node ${winner} wins and activates. Node ${winner === "A" ? "B" : "A"} gets a precondition failure, re-reads v42, checks the winner's lease, and forwards. Epoch becomes 8 once, not twice.` : "Both nodes can read the same version. Only one conditional write can replace it."}</div>
  </div>;
}

export function RestoreLab() {
  const [newEpoch, setNewEpoch] = useState(true);
  const [gap, setGap] = useState(false);
  const entries = [
    { epoch: 7, kind: "snapshot", txid: 40 },
    ...[41, 42, 43].map(txid => ({ epoch: 7, kind: "segment", txid })),
    ...(newEpoch ? [{ epoch: 8, kind: "snapshot", txid: 42 }] : []),
    ...[43, ...(gap ? [] : [44]), 45].map(txid => ({ epoch: 8, kind: "segment", txid }))
  ];
  const plan = restorePlan(entries);
  return <div className="interactive">
    <div className="panel-bar"><span className="mini-label">Restore planner / mirrors plan_restore</span><Badge>Try a partial epoch</Badge></div>
    <div className="check-controls"><label><input type="checkbox" checked={newEpoch} onChange={e => setNewEpoch(e.target.checked)}/> Epoch 8 has an activation snapshot</label><label><input type="checkbox" checked={gap} onChange={e => setGap(e.target.checked)}/> Remove segment 44 from epoch 8</label></div>
    {[7, 8].map(epoch => <div className={`epoch-row ${plan.epoch === epoch ? "chosen" : ""}`} key={epoch}><div className="epoch-name"><b>e{epoch}</b><small>{plan.epoch === epoch ? "selected epoch" : "not selected"}</small></div><div className="segment-row">{entries.filter(e => e.epoch === epoch).map(e => { const used = plan.epoch === epoch && (e.kind === "snapshot" ? e.txid === plan.snapshot : plan.segments.includes(e.txid)); return <div key={`${e.kind}-${e.txid}`} className={`segment ${e.kind} ${used ? "used" : ""}`}><Icon name={e.kind === "snapshot" ? "database" : "layers"} size={19}/><strong>{e.kind === "snapshot" ? "Snapshot" : "Segment"} {e.txid}</strong><small>{used ? "restore" : "ignore"}</small></div>; })}</div></div>)}
    <div className="restore-result" aria-live="polite"><Icon name="database" size={30}/><div><strong>Restore e{plan.epoch} to transaction {plan.txid}</strong><p>Snapshot {plan.snapshot}{plan.segments.length ? ` + segments ${plan.segments.join(", ")}` : ""}. Then upload a snapshot into the newly acquired epoch before serving.</p></div></div>
    {gap && newEpoch && <Note title="A gap is a hard stop in the planner" tone="amber">The current planner stops at the first missing transaction; it does not skip 44 to apply 45, or merge history from epoch 7. This is an illustration of the algorithm, not a supported way to delete production segments.</Note>}
    {!newEpoch && <Note title="Segments alone cannot establish an epoch">Even though epoch 8 has segment objects, it has no snapshot. plan_restore selects epoch 7. An activation snapshot is what makes an epoch a usable recovery base.</Note>}
    <Source files="crates/ltx/src/lib.rs:224-266; crates/node/src/actor.rs:58-132"/>
  </div>;
}

export function FailureLab() {
  const [selected, setSelected] = useState(2);
  const point = crashPoints[selected];
  return <div className="interactive">
    <div className="panel-bar"><span className="mini-label">Crash-window explorer</span><Badge tone="amber">SQL outcome ≠ external outcome</Badge></div>
    <div className="crash-track">{crashPoints.map((p, i) => <button key={p.label} onClick={() => setSelected(i)} aria-pressed={selected === i}><span className="crash-dot">{i + 1}</span>{p.label}</button>)}</div>
    <div className="failure-result" aria-live="polite"><Badge tone={point.tone}>{selected === 3 ? "KNOWN SUCCESS IF RECEIVED" : selected === 2 ? "UNKNOWN TO CLIENT" : "SPECIFIC ILLUSTRATED CRASH"}</Badge><h3>{point.title}</h3><p>{point.result}</p><div className="grid two"><div><h4>What recovery does</h4><p>{point.recovery}</p></div><div><h4>What the application does</h4><p>{point.retry}</p></div></div></div>
  </div>;
}

export const patterns = {
  agents: {
    title: "An agent is a durable run, not a forever-running process.",
    key: "agent-run / run-8c21",
    states: ["ready", "tool-pending", "waiting", "complete"],
    driver: "External orchestrator / worker",
    data: "runs, steps, messages, tool_intents, processed_events",
    flow: ["Driver submits event", "Actor persists one transition", "Worker performs tool call", "Actor accepts result token"],
    steps: [
      ["Choose the key", "Use a run or conversation ID. Store prompts, messages, decisions, and step status in its private database. Guest memory is only a resident cache."],
      ["Keep turns short", "A plan step records a durable tool intent and returns. Do not hold the actor slot across a long model stream or human approval; the default method deadline is finite."],
      ["Drive progress externally", "A worker polls or receives work through application infrastructure, calls the model/tool with an idempotency key, then reports a result with the expected step token."],
      ["Resume safely", "On worker restart, scan unfinished intents and retry them. On node restart, the next request restores the actor. These are two separate recovery mechanisms."]
    ],
    missing: "StateX does not supply an agent scheduler, model adapter, token stream, background continuation, or durable tool delivery.",
    code: `advance(event_id, expected_step, result):\n  if processed_events contains event_id:\n    return saved_reply\n  require run.step == expected_step\n  persist result and next tool_intent\n  save event_id and reply\n  return next_step  # host commits + replicates`,
    risk: "A tool can finish while its result callback is lost. Deduplicate the tool request externally and the callback inside the actor; neither layer alone is enough."
  },
  queues: {
    title: "A queue shard is a serialized claim ledger.",
    key: "queue-shard / emails-03",
    states: ["ready", "claimed", "acked", "retry / dead"],
    driver: "External consumers / pollers",
    data: "jobs, enqueue_keys, claims, attempts, dead_letters",
    flow: ["Producer enqueues", "Consumer claims a job", "Work happens outside", "Consumer acknowledges token"],
    steps: [
      ["Choose a shard", "One key owns one queue shard. enqueue deduplicates a producer request and inserts a ready job atomically. Multiple keys add parallelism, not a global order."],
      ["Claim in SQL", "claim selects an eligible job and records a fresh claim token, worker ID, attempt, and visibility deadline in the same transaction."],
      ["Work outside the actor", "The consumer performs the job without holding the actor call open. Its claim deadline is an application lease, distinct from StateX's node lease."],
      ["Acknowledge conditionally", "ack succeeds only for the current job token. An expired claim can be reclaimed by a later call; stale worker acknowledgements must not complete a newer attempt."]
    ],
    missing: "StateX does not supply consumers, automatic visibility scanning, push delivery, backpressure policy, dead-letter policy, or global FIFO ordering.",
    code: `claim(worker, now):\n  job = first ready or expired job\n  token = fresh application claim token\n  persist job.claim(token, worker, deadline)\n  return job, token\n\nack(job_id, token):\n  require token == current_claim_token\n  persist completed(job_id)`,
    risk: "Visibility expiry creates at-least-once processing. Even with token-fenced acknowledgements, old and new workers can both affect an external service unless that service deduplicates or fences work."
  },
  cron: {
    title: "A schedule is a durable cursor over time.",
    key: "schedule / daily-report",
    states: ["scheduled", "due", "dispatched", "next due"],
    driver: "External time source / tick service",
    data: "schedules, occurrences, dispatch_intents, delivery_attempts",
    flow: ["Ticker calls due()", "Actor records occurrence", "Dispatcher delivers work", "Actor advances or retries"],
    steps: [
      ["Store schedule state", "Persist the rule, time zone, next_due, and missed-run policy. Pick a deterministic occurrence ID such as (schedule_id, scheduled_instant)."],
      ["Send ticks from outside", "An external scheduler invokes the actor. A node lease renewal loop is not a guest timer; an evicted actor cannot wake itself on a deadline."],
      ["Create one durable occurrence", "Insert the occurrence under a uniqueness constraint and record its dispatch intent. Compute the next cursor according to an explicit skip, catch-up, or coalesce policy."],
      ["Deliver and retry", "An external dispatcher retries pending intents and calls the target with the occurrence ID. Target-side deduplication closes the duplicate-delivery window."]
    ],
    missing: "StateX does not supply cron parsing, time-zone/DST rules, scheduled wakeups, an alarm API, or an exactly-once dispatch service.",
    code: `tick(now):\n  for each due occurrence within bounded batch:\n    id = (schedule_id, scheduled_instant)\n    insert occurrence + dispatch_intent if absent\n  persist next_due\n  return pending_dispatches\n\n# A separate dispatcher retries delivery by id.`,
    risk: "Wall-clock changes, DST, and missed ticks are application semantics. Bound catch-up batches and decide whether a delayed occurrence is skipped or delivered."
  },
  workflows: {
    title: "A workflow is a sequence of idempotent local commits.",
    key: "workflow / order-409",
    states: ["started", "reserved", "charged", "done / compensate"],
    driver: "External workflow runner",
    data: "workflow_state, completed_steps, commands, compensations",
    flow: ["Runner requests transition", "Actor records command", "Target commits separately", "Runner records completion"],
    steps: [
      ["Define the boundary", "Keep the workflow status in one actor, but recognize that inventory, payment, and shipping may each have their own transaction boundary."],
      ["Persist intentions", "Write the next command and its stable step ID before external delivery. A durable outbox-style table is application code, not built-in dispatch."],
      ["Make each participant idempotent", "Send (workflow_id, step_id) to each target. A target stores a previously produced result and returns it on duplicate commands."],
      ["Compensate explicitly", "If a later step fails, issue a compensating command, such as releasing a reservation. This is a new operation, not a rollback of the earlier actor."]
    ],
    missing: "StateX does not supply a workflow DSL, saga coordinator, cross-actor transaction, built-in outbox relay, or distributed deadlock detector.",
    code: `advance(expected_state, step_id):\n  require workflow.state == expected_state\n  persist command(step_id, target, payload)\n  persist workflow.state = waiting\n  return command\n\ncomplete(step_id, result):\n  deduplicate and persist next state`,
    risk: "A synchronous A → B call does not join transactions. B can commit even when A later errors; use stable step IDs and explicit reconciliation."
  }
};

export function PatternLab() {
  const [active, setActive] = useState("agents");
  const p = patterns[active];
  return <div className="pattern-lab">
    <div className="pattern-tabs" aria-label="System pattern">{Object.keys(patterns).map(id => <button aria-pressed={id === active} key={id} onClick={() => setActive(id)}><Icon name={{ agents: "cube", queues: "layers", cron: "clock", workflows: "branch" }[id]}/>{id[0].toUpperCase() + id.slice(1)}</button>)}</div>
    <div className="pattern-content" aria-live="polite"><Badge tone="amber">APPLICATION DESIGN / NOT A BUILT-IN SUBSYSTEM</Badge><h2>{p.title}</h2><div className="pattern-identity"><div><span className="mini-label">Actor type / key</span><code>{p.key}</code></div><div><span className="mini-label">Who wakes it?</span><strong>{p.driver}</strong></div></div>
      <Flow label={`${active} processing sequence`} nodes={p.flow.map((title, i) => ({ title, tone: i === 1 ? "mint" : "", kicker: i === 1 ? "STATEX TRANSACTION" : "APPLICATION INFRASTRUCTURE" }))}/>
      <div className="state-machine" aria-label={`${active} example states`}>{p.states.map((s, i) => <React.Fragment key={s}>{i > 0 && <Icon name="arrow" size={17}/>}<span>{s}</span></React.Fragment>)}</div>
      <div className="grid two"><div>{p.steps.map(([title, text], i) => <div className="numbered-item" key={title}><span>{i + 1}</span><div><h3>{title}</h3><p>{text}</p></div></div>)}</div><div><Code title="Protocol sketch / pseudocode, not a StateX API">{p.code}</Code><div className="table-note"><strong>Example tables</strong><p><code>{p.data}</code></p></div><Note title="Failure to design for" tone="amber">{p.risk}</Note></div></div>
      <Note title="What you still have to build" tone="amber">{p.missing}</Note>
    </div>
  </div>;
}
