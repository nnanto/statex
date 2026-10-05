import React, { useState } from "react";
import { createRoot } from "react-dom/client";
import { chapters } from "./model.mjs";
import { pages } from "./pages.jsx";
import { Icon } from "./ui.jsx";

class GuideBoundary extends React.Component {
  state = { error: null };
  static getDerivedStateFromError(error) { return { error }; }
  render() {
    if (this.state.error) return <main className="app-error"><h1>The guide could not render.</h1><p>{this.state.error.message}</p><p>Rebuild from the visual-guide folder with <code>npm run build</code>, then reopen the generated HTML.</p></main>;
    return this.props.children;
  }
}

function App() {
  const pageId = document.documentElement.dataset.page || "index";
  const chapterIndex = chapters.findIndex(c => c.id === pageId);
  const chapter = chapters[chapterIndex];
  const [menuOpen, setMenuOpen] = useState(false);
  const [search, setSearch] = useState("");
  if (!chapter) throw new Error(`Unknown guide chapter: ${pageId}`);
  const Page = pages[pageId];
  const visible = chapters.filter(c => `${c.label} ${c.title} ${c.subtitle}`.toLowerCase().includes(search.toLowerCase()));
  const groups = [...new Set(chapters.map(c => c.group))];
  return <>
    <a className="skip-link" href="#main">Skip to content</a>
    <div className="mobile-top"><a href="index.html" className="brand"><span className="brand-mark"><Icon name="layers"/></span>statex <span>field guide</span></a><button aria-label={menuOpen ? "Close chapter menu" : "Open chapter menu"} aria-expanded={menuOpen} aria-controls="chapter-sidebar" onClick={() => setMenuOpen(!menuOpen)}><Icon name="menu"/></button></div>
    <aside id="chapter-sidebar" className={`sidebar ${menuOpen ? "open" : ""}`}>
      <a className="brand" href="index.html"><span className="brand-mark"><Icon name="layers" size={25}/></span>statex<span className="brand-dot">.</span></a>
      <div className="sidebar-subtitle">THE VISUAL FIELD GUIDE</div>
      <label className="nav-search"><Icon name="search" size={16}/><input type="search" aria-label="Find a chapter" placeholder="Find a chapter..." value={search} onChange={e => setSearch(e.target.value)}/></label>
      <nav aria-label="Chapters">{groups.map(group => {
        const items = visible.filter(c => c.group === group);
        if (!items.length) return null;
        return <div className="nav-group" key={group}><div className="nav-group-label">{group}</div>{items.map(c => <a key={c.id} href={`${c.id}.html`} aria-current={c.id === pageId ? "page" : undefined}><span className="nav-number">{String(chapters.indexOf(c) + 1).padStart(2, "0")}</span><span>{c.label}</span>{c.id === pageId && <span className="nav-active-dot"/>}</a>)}</div>;
      })}{!visible.length && <p className="nav-empty" role="status">No chapters found.</p>}</nav>
      <div className="sidebar-bottom"><div className="live-dot"/><div><strong>Built from the source</strong><span>Working-tree edition · Oct 2026</span></div></div>
    </aside>
    <div className="main-shell">
      <header className="topbar"><div><span className="topbar-label">SYSTEMS, EXPLAINED</span><span className="topbar-divider">/</span><span>{chapter.label}</span></div><button className="print-button" onClick={() => window.print()}><Icon name="book" size={16}/> Print chapter</button></header>
      <main id="main" tabIndex="-1">
        <div className="chapter-heading"><div className="chapter-meta"><span className="eyebrow">Chapter {String(chapterIndex + 1).padStart(2, "0")} <span className="muted">/ {chapters.length}</span></span><div className="depth-meter" aria-label={`Depth ${chapter.depth} of 5`}><span>Depth</span>{Array.from({ length: 6 }, (_, i) => <span className={`depth-line ${i <= chapter.depth ? "filled" : ""}`} key={i}/>)}</div></div><h1>{chapter.title}</h1><p className="page-subtitle">{chapter.subtitle}</p></div>
        <Page/>
        <footer className="chapter-footer"><div className="footer-nav">{chapterIndex > 0 ? <a className="prev-chapter" href={`${chapters[chapterIndex - 1].id}.html`}><span>PREVIOUS CHAPTER</span><strong>{chapters[chapterIndex - 1].label}</strong></a> : <div className="footer-start"><Icon name="layers"/><span>Start broad.<br/>Go one layer deeper.</span></div>}{chapterIndex < chapters.length - 1 ? <a className="next-chapter" href={`${chapters[chapterIndex + 1].id}.html`}><span>NEXT CHAPTER</span><strong>{chapters[chapterIndex + 1].label}<Icon name="arrow"/></strong></a> : <a className="next-chapter" href="index.html"><span>BACK TO THE OUTSIDE</span><strong>The big picture <Icon name="arrow"/></strong></a>}</div><p>StateX visual field guide · React · Offline-ready · Explanatory diagrams, not a live control plane</p></footer>
      </main>
    </div>
  </>;
}

createRoot(document.getElementById("root")).render(<GuideBoundary><App/></GuideBoundary>);
