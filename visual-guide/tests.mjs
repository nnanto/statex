import assert from "node:assert/strict";
import { readFile, access } from "node:fs/promises";
import { JSDOM, VirtualConsole } from "jsdom";
import { chapters, leasePhase, restorePlan, errors } from "./src/model.mjs";

let passed = 0;
const wait = () => new Promise(resolve => setTimeout(resolve, 25));
async function test(name, run) {
  await run();
  passed += 1;
  console.log(`PASS ${name}`);
}
async function withPage(id, run) {
  const html = await readFile(new URL(`./dist/${id}.html`, import.meta.url), "utf8");
  const problems = [];
  const console = new VirtualConsole();
  console.on("jsdomError", e => problems.push(e.message));
  console.on("error", (...args) => problems.push(args.join(" ")));
  const dom = new JSDOM(html, {
    url: `file:///statex-guide/${id}.html`,
    runScripts: "dangerously",
    pretendToBeVisual: true,
    virtualConsole: console
  });
  try {
    await wait();
    const document = dom.window.document;
    const click = async label => {
      const button = [...document.querySelectorAll("button")].find(b => b.textContent.trim() === label || b.getAttribute("aria-label") === label);
      assert.ok(button, `Button "${label}" exists`);
      assert.ok(!button.disabled, `Button "${label}" is enabled`);
      button.click();
      await wait();
    };
    const setValue = async (label, value) => {
      const input = document.querySelector(`[aria-label="${label}"]`);
      assert.ok(input, `Input "${label}" exists`);
      const proto = input.tagName === "SELECT" ? dom.window.HTMLSelectElement.prototype : dom.window.HTMLInputElement.prototype;
      Object.getOwnPropertyDescriptor(proto, "value").set.call(input, value);
      input.dispatchEvent(new dom.window.Event("input", { bubbles: true }));
      input.dispatchEvent(new dom.window.Event("change", { bubbles: true }));
      await wait();
    };
    await run({ document, window: dom.window, click, setValue, html });
    assert.deepEqual(problems, [], `No browser or React errors on ${id}`);
  } finally {
    dom.window.close();
  }
}

await test("lease boundaries, strict margin, and minimum margin", () => {
  assert.equal(leasePhase(7.9, 10).phase, "valid");
  assert.equal(leasePhase(8, 10).phase, "gap");
  assert.equal(leasePhase(9.9, 10).phase, "gap");
  assert.equal(leasePhase(10, 10).phase, "takeover");
  assert.equal(leasePhase(0, 0.5).margin, 0.2);
  assert.equal(leasePhase(0.3, 0.5).phase, "gap");
});
await test("restore uses latest snapshot in latest snapshot-bearing epoch", () => {
  const plan = restorePlan([
    { epoch: 1, kind: "snapshot", txid: 0 },
    { epoch: 2, kind: "snapshot", txid: 4 },
    { epoch: 2, kind: "snapshot", txid: 8 },
    { epoch: 2, kind: "segment", txid: 10 },
    { epoch: 2, kind: "segment", txid: 9 },
    { epoch: 3, kind: "segment", txid: 11 }
  ]);
  assert.deepEqual(plan, { epoch: 2, snapshot: 8, segments: [9, 10], txid: 10 });
  assert.equal(restorePlan([]), null);
  assert.equal(restorePlan([{ epoch: 1, kind: "segment", txid: 1 }]), null);
});
await test("restore stops at a gap and never mixes epochs", () => {
  assert.deepEqual(restorePlan([
    { epoch: 7, kind: "snapshot", txid: 40 },
    { epoch: 7, kind: "segment", txid: 44 },
    { epoch: 8, kind: "snapshot", txid: 42 },
    { epoch: 8, kind: "segment", txid: 43 },
    { epoch: 8, kind: "segment", txid: 45 }
  ]), { epoch: 8, snapshot: 42, segments: [43], txid: 43 });
});

for (const chapter of chapters) {
  await test(`${chapter.id}: real page, offline bundle, navigation, semantics`, () => withPage(chapter.id, async ({ document, html }) => {
    assert.equal(document.documentElement.lang, "en");
    assert.equal(document.querySelectorAll("h1").length, 1);
    assert.equal(document.querySelector("h1").textContent, chapter.title);
    assert.equal(document.querySelector('nav a[aria-current="page"]').getAttribute("href"), `${chapter.id}.html`);
    assert.equal(document.querySelectorAll('nav[aria-label="Chapters"] a').length, chapters.length);
    assert.ok(document.querySelector("main").textContent.length > 2500, "Substantive chapter content");
    assert.ok(document.querySelector(".sources"), "Source attribution present");
    assert.equal(document.querySelectorAll("script[src], link[rel=stylesheet], iframe, img[src]").length, 0, "No remote assets");
    assert.ok(!/@import\s|url\(\s*["']?https?:/i.test(document.querySelector("style").textContent));
    assert.ok(!html.includes("height=\" sixty\""), "No malformed SVG dimension");
    assert.ok(html.includes("Permission is hereby granted, free of charge"), "Bundled React license retained");
    for (const link of document.querySelectorAll("a[href]")) {
      const href = link.getAttribute("href");
      if (href.startsWith("#")) {
        assert.ok(document.getElementById(href.slice(1)), `Anchor ${href} exists`);
      } else {
        assert.ok(chapters.some(c => href === `${c.id}.html`), `Link ${href} stays in guide`);
        await access(new URL(`./dist/${href}`, import.meta.url));
      }
    }
    for (const svg of document.querySelectorAll('svg[role="img"]')) {
      assert.ok(svg.getAttribute("aria-label") || svg.getAttribute("aria-labelledby"), "Diagram has an accessible description");
    }
    assert.equal(document.querySelectorAll(".app-error").length, 0);
  }));
}

await test("overview: all onion layers and both topology states", () => withPage("index", async ({ document, click }) => {
  for (const [label, text] of [
    ["02 Routing", "Any node is a front door"],
    ["03 Ownership", "A lease plus an epoch"],
    ["04 Transaction", "One call, one SQLite boundary"],
    ["05 Durability", "Pages before acknowledgement"],
    ["01 Application", "A durable object per key"]
  ]) {
    await click(label);
    assert.equal(document.querySelector(".layer-description h3").textContent, text);
  }
  await click("First touch");
  assert.match(document.querySelector(".diagram-caption").textContent, /conditional write/);
  await click("Existing actor");
  assert.match(document.querySelector(".diagram-caption").textContent, /front door/);
}));

await test("architecture: all eight component contracts selectable", () => withPage("architecture", async ({ document }) => {
  const buttons = [...document.querySelectorAll(".component-list button")];
  assert.equal(buttons.length, 8);
  for (const button of buttons) {
    button.click();
    await wait();
    assert.equal(button.getAttribute("aria-pressed"), "true");
    assert.ok(document.querySelector(".component-detail h3").textContent.length > 5);
    assert.ok(document.querySelector(".component-detail").textContent.includes("Boundary"));
  }
}));

await test("request: eight steps, boundary controls, read and rollback branches", () => withPage("journey", async ({ document, click }) => {
  assert.ok([...document.querySelectorAll("button")].find(b => b.textContent === "Previous step").disabled);
  for (let i = 1; i < 8; i += 1) await click("Next step");
  assert.equal(document.querySelector(".step-body h3").textContent, "Reply");
  assert.ok([...document.querySelectorAll("button")].find(b => b.textContent.trim() === "Next step").disabled);
  await click("Read-only");
  assert.equal(document.querySelector(".step-body h3").textContent, "No new WAL pages");
  await click("Method error");
  assert.equal(document.querySelector(".step-body h3").textContent, "Rollback");
  await click("Previous step");
  assert.equal(document.querySelector(".step-number").textContent, "07");
  await click("Write");
  assert.equal(document.querySelector(".step-body h3").textContent, "Check");
}));

await test("leases: slider, TTL, CAS winners and reset", () => withPage("leases", async ({ document, click, setValue }) => {
  await setValue("Elapsed lease time", "8");
  assert.match(document.querySelector(".lease-explanation").textContent, /must not acknowledge/);
  await setValue("Elapsed lease time", "10");
  assert.match(document.querySelector(".lease-explanation").textContent, /compete for ownership/);
  await setValue("Lease TTL", "1");
  assert.equal(document.querySelector('[aria-label="Elapsed lease time"]').max, "1.2");
  await setValue("Elapsed lease time", "0.8");
  assert.match(document.querySelector(".lease-explanation").textContent, /must not acknowledge/);
  await click("Let A reach CAS first");
  assert.match(document.querySelector(".race-result").textContent, /Node A wins/);
  await click("Reset race");
  await click("Let B reach CAS first");
  assert.match(document.querySelector(".race-result").textContent, /Node B wins/);
}));

await test("recovery: all combinations of snapshot availability and gaps", () => withPage("durability", async ({ document }) => {
  const [snapshot, gap] = document.querySelectorAll('input[type="checkbox"]');
  assert.match(document.querySelector(".restore-result").textContent, /transaction 45/);
  gap.click(); await wait();
  assert.match(document.querySelector(".restore-result").textContent, /transaction 43/);
  snapshot.click(); await wait();
  assert.match(document.querySelector(".restore-result").textContent, /Restore e7/);
  gap.click(); await wait();
  assert.match(document.querySelector(".restore-result").textContent, /Restore e7/);
  snapshot.click(); await wait();
  assert.match(document.querySelector(".restore-result").textContent, /transaction 45/);
}));

await test("failures: crash windows, filter, empty state and complete status table", () => withPage("failures", async ({ document, click, setValue }) => {
  for (const button of document.querySelectorAll(".crash-track button")) {
    button.click(); await wait();
    assert.equal(button.getAttribute("aria-pressed"), "true");
  }
  assert.match(document.querySelector(".failure-result").textContent, /acknowledged write survives/);
  await setValue("Filter failure conditions", "compaction");
  assert.equal(document.querySelectorAll(".section")[1].querySelectorAll("tbody tr").length, 1);
  await setValue("Filter failure conditions", "no-such-failure");
  assert.match(document.querySelector("main").textContent, /No matching failure conditions/);
  await setValue("Filter failure conditions", "");
  for (const [code] of errors) assert.ok([...document.querySelectorAll("td")].some(td => td.textContent === code));
}));

await test("use cases: implemented and proposed examples are separated", () => withPage("use-cases", async ({ document, click }) => {
  assert.equal(document.querySelectorAll(".use-case").length, 6);
  await click("In this repository");
  assert.equal(document.querySelectorAll(".use-case").length, 3);
  assert.ok([...document.querySelectorAll(".use-case .badge")].every(b => b.textContent === "Repository example"));
  await click("Design ideas");
  assert.equal(document.querySelectorAll(".use-case").length, 3);
  assert.ok([...document.querySelectorAll(".use-case .badge")].every(b => b.textContent === "Application design"));
  await click("All use cases");
  assert.equal(document.querySelectorAll(".use-case").length, 6);
}));

await test("patterns: agents, queues, cron and workflows have explicit external drivers", () => withPage("patterns", async ({ document, click }) => {
  for (const name of ["Agents", "Queues", "Cron", "Workflows"]) {
    await click(name);
    const content = document.querySelector(".pattern-content");
    assert.match(content.textContent, /APPLICATION DESIGN \/ NOT A BUILT-IN SUBSYSTEM/);
    assert.match(content.textContent, /External/);
    assert.match(content.textContent, /What you still have to build/);
    assert.equal(content.querySelectorAll(".numbered-item").length, 4);
    assert.equal(content.querySelectorAll(".state-machine>span").length, 4);
  }
}));

await test("reference: glossary, empty results, chapter search and mobile menu", () => withPage("reference", async ({ document, click, setValue }) => {
  await setValue("Search glossary", "ETag");
  assert.equal(document.querySelectorAll(".glossary>div").length, 2);
  await setValue("Search glossary", "does-not-exist");
  assert.match(document.querySelector("main").textContent, /No matching terms/);
  await setValue("Search glossary", "");
  assert.equal(document.querySelectorAll(".glossary>div").length, 24);
  await setValue("Find a chapter", "cron");
  assert.equal(document.querySelectorAll('nav[aria-label="Chapters"] a').length, 1);
  await setValue("Find a chapter", "nothing-matches");
  assert.match(document.querySelector(".nav-empty").textContent, /No chapters found/);
  await setValue("Find a chapter", "");
  await click("Open chapter menu");
  assert.ok(document.querySelector(".sidebar").classList.contains("open"));
  await click("Close chapter menu");
  assert.ok(!document.querySelector(".sidebar").classList.contains("open"));
}));

console.log(`\n${passed} checks passed. All ${chapters.length} chapters render offline without React errors.`);
