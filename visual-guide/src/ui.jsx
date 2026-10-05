import React from "react";

export function Icon({ name = "cube", size = 22, ...props }) {
  const paths = {
    cube: <><path d="m12 3 9 5v8l-9 5-9-5V8Z"/><path d="m3 8 9 5 9-5M12 13v8M7.5 5.5l9 5"/></>,
    arrow: <><path d="M4 12h16m-6-6 6 6-6 6"/></>,
    layers: <><path d="m12 3 10 5-10 5L2 8Z"/><path d="m2 12 10 5 10-5M2 16l10 5 10-5"/></>,
    database: <><ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v14c0 4 16 4 16 0V5M4 12c0 4 16 4 16 0"/></>,
    clock: <><circle cx="12" cy="12" r="9"/><path d="M12 6v6l4 2"/></>,
    shield: <><path d="m12 2 8 4v6c0 5-8 10-8 10S4 17 4 12V6Z"/><path d="m8 12 3 3 5-6"/></>,
    code: <><path d="m7 6-6 6 6 6m10-12 6 6-6 6M14 3l-4 18"/></>,
    branch: <><circle cx="6" cy="5" r="2"/><circle cx="6" cy="19" r="2"/><circle cx="18" cy="5" r="2"/><path d="M6 7v10m0-5c8 0 12-1 12-5"/></>,
    search: <><circle cx="10" cy="10" r="6"/><path d="m15 15 6 6"/></>,
    check: <path d="m5 12 4 4L19 6"/>,
    alert: <><path d="m12 3 10 18H2Z"/><path d="M12 9v5m0 3v1"/></>,
    book: <><path d="M12 5C9 2 3 3 2 4v16c4-2 7-2 10 0 3-2 6-2 10 0V4c-1-1-7-2-10 1Zm0 0v15"/></>,
    menu: <path d="M3 6h18M3 12h18M3 18h18"/>
  };
  return <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" {...props}>{paths[name] || paths.cube}</svg>;
}

export function Section({ eyebrow, title, children, id, className = "" }) {
  return <section id={id} className={`section ${className}`}>{eyebrow && <div className="eyebrow">{eyebrow}</div>}{title && <h2>{title}</h2>}{children}</section>;
}

export function Card({ title, children, icon = "cube", tone = "", className = "" }) {
  return <article className={`card ${tone} ${className}`}><div className="card-icon"><Icon name={icon}/></div><h3>{title}</h3>{children}</article>;
}

export function Note({ title, children, tone = "mint" }) {
  return <aside className={`note ${tone}`}><Icon name={tone === "amber" || tone === "red" ? "alert" : "shield"}/><div><strong>{title}</strong><div>{children}</div></div></aside>;
}

export function Source({ files }) {
  return <div className="sources"><Icon name="code" size={14}/><span>Source: {files}</span></div>;
}

export function Code({ title, children }) {
  return <figure className="code-block">{title && <figcaption><span className="code-dots">● ● ●</span>{title}</figcaption>}<pre><code>{children}</code></pre></figure>;
}

export function Table({ headers, rows }) {
  return <div className="table-wrap" tabIndex="0" role="region" aria-label={headers.join(", ")}><table><thead><tr>{headers.map(h => <th key={h} scope="col">{h}</th>)}</tr></thead><tbody>{rows.map((row, i) => <tr key={i}>{row.map((c, j) => <td key={j}>{c}</td>)}</tr>)}</tbody></table></div>;
}

export function Flow({ nodes, label = "Sequence", vertical = false }) {
  return <figure aria-label={label} className={`flow ${vertical ? "vertical" : ""}`}><figcaption className="sr-only">{label}</figcaption>{nodes.map((n, i) => <React.Fragment key={i}>{i > 0 && <div className="flow-arrow"><Icon name="arrow"/></div>}<div className={`flow-node ${n.tone || ""}`}><span className="mini-label">{n.kicker || String(i + 1).padStart(2, "0")}</span><strong>{n.title || n}</strong>{n.text && <small>{n.text}</small>}</div></React.Fragment>)}</figure>;
}

export function Detail({ title, children }) {
  return <details className="detail"><summary>{title}<span aria-hidden="true">+</span></summary><div>{children}</div></details>;
}

export function Badge({ children, tone = "" }) {
  return <span className={`badge ${tone}`}>{children}</span>;
}

export function ChapterLink({ id, children }) {
  return <a className="text-link" href={`${id}.html`}>{children}<Icon name="arrow" size={16}/></a>;
}
