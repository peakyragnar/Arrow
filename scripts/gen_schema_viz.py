"""Generate a self-contained HTML visualization of the live schema.

Reads Postgres (pg_catalog + information_schema) and emits
`arrow_db_schema.html` with:
  - a full Mermaid ERD (every column, PK/FK/UK markers, NOT NULL flag)
  - per-table detail cards (columns, FKs, UNIQUE, CHECK, indexes)
  - a legend

The live database is the source of truth. Re-run this after any migration:
    uv run scripts/gen_schema_viz.py

Serve locally (Live Server or `python -m http.server`) and refresh the page.
"""

from __future__ import annotations

import html
from collections import defaultdict
from pathlib import Path

from arrow.db.connection import get_conn

EXCLUDED_TABLES = {"schema_migrations"}

OUT_PATH = Path(__file__).resolve().parents[1] / "arrow_db_schema.html"


# ---------------------------------------------------------------------------
# Introspection
# ---------------------------------------------------------------------------


def fetch_columns(conn) -> dict[str, list[dict]]:
    sql = """
        SELECT
            c.relname                               AS table_name,
            a.attname                               AS column_name,
            a.attnum                                AS ordinal_position,
            format_type(a.atttypid, a.atttypmod)    AS data_type,
            a.attnotnull                            AS not_null,
            pg_get_expr(d.adbin, d.adrelid)         AS default_expr
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid
        LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
        WHERE n.nspname = 'public'
          AND c.relkind = 'r'
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY c.relname, a.attnum;
    """
    out: dict[str, list[dict]] = defaultdict(list)
    for row in conn.execute(sql):
        table, col, pos, dtype, notnull, default = row
        if table in EXCLUDED_TABLES:
            continue
        out[table].append({
            "name": col,
            "position": pos,
            "type": dtype,
            "not_null": notnull,
            "default": default,
        })
    return dict(out)


def fetch_primary_keys(conn) -> dict[str, list[str]]:
    sql = """
        SELECT
            c.relname AS table_name,
            a.attname AS column_name,
            array_position(i.indkey::int[], a.attnum::int) AS key_pos
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
        WHERE n.nspname = 'public'
          AND i.indisprimary
        ORDER BY c.relname, key_pos;
    """
    out: dict[str, list[str]] = defaultdict(list)
    for row in conn.execute(sql):
        table, col, _pos = row
        if table in EXCLUDED_TABLES:
            continue
        out[table].append(col)
    return dict(out)


def fetch_foreign_keys(conn) -> list[dict]:
    sql = """
        SELECT
            con.conname             AS constraint_name,
            cls.relname             AS table_name,
            rcls.relname            AS foreign_table,
            con.conkey              AS conkey,
            con.confkey             AS confkey,
            CASE con.confdeltype
                WHEN 'a' THEN 'NO ACTION'
                WHEN 'r' THEN 'RESTRICT'
                WHEN 'c' THEN 'CASCADE'
                WHEN 'n' THEN 'SET NULL'
                WHEN 'd' THEN 'SET DEFAULT'
            END AS on_delete,
            con.conrelid            AS conrelid,
            con.confrelid           AS confrelid
        FROM pg_constraint con
        JOIN pg_class cls   ON cls.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = cls.relnamespace
        JOIN pg_class rcls  ON rcls.oid = con.confrelid
        WHERE con.contype = 'f'
          AND n.nspname = 'public'
        ORDER BY cls.relname, con.conname;
    """

    def resolve_columns(conn, relid: int, attnums: list[int]) -> list[str]:
        rows = conn.execute(
            "SELECT attnum, attname FROM pg_attribute "
            "WHERE attrelid = %s AND attnum = ANY(%s::int[])",
            (relid, attnums),
        ).fetchall()
        by_num = {n: name for n, name in rows}
        return [by_num[n] for n in attnums]

    fks: list[dict] = []
    for row in conn.execute(sql):
        name, table, ftable, conkey, confkey, on_delete, conrelid, confrelid = row
        if table in EXCLUDED_TABLES:
            continue
        cols = resolve_columns(conn, conrelid, conkey)
        fcols = resolve_columns(conn, confrelid, confkey)
        fks.append({
            "name": name,
            "table": table,
            "columns": cols,
            "foreign_table": ftable,
            "foreign_columns": fcols,
            "on_delete": on_delete,
        })
    return fks


def fetch_unique_constraints(conn) -> list[dict]:
    """Table-level UNIQUE constraints only (partial unique indexes are in fetch_indexes)."""
    sql = """
        SELECT con.conname, cls.relname, pg_get_constraintdef(con.oid)
        FROM pg_constraint con
        JOIN pg_class cls   ON cls.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = cls.relnamespace
        WHERE con.contype = 'u'
          AND n.nspname = 'public'
        ORDER BY cls.relname, con.conname;
    """
    out = []
    for name, table, defn in conn.execute(sql):
        if table in EXCLUDED_TABLES:
            continue
        out.append({"name": name, "table": table, "definition": defn})
    return out


def fetch_check_constraints(conn) -> list[dict]:
    sql = """
        SELECT con.conname, cls.relname, pg_get_constraintdef(con.oid)
        FROM pg_constraint con
        JOIN pg_class cls   ON cls.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = cls.relnamespace
        WHERE con.contype = 'c'
          AND n.nspname = 'public'
        ORDER BY cls.relname, con.conname;
    """
    out = []
    for name, table, defn in conn.execute(sql):
        if table in EXCLUDED_TABLES:
            continue
        out.append({"name": name, "table": table, "definition": defn})
    return out


def fetch_indexes(conn) -> list[dict]:
    """All indexes. Primary-key and constraint-backed indexes are flagged."""
    sql = """
        SELECT
            cls.relname                        AS table_name,
            ic.relname                         AS index_name,
            pg_get_indexdef(i.indexrelid)      AS definition,
            i.indisunique                      AS is_unique,
            i.indisprimary                     AS is_primary,
            (con.contype IS NOT NULL)          AS from_constraint
        FROM pg_index i
        JOIN pg_class cls   ON cls.oid = i.indrelid
        JOIN pg_class ic    ON ic.oid = i.indexrelid
        JOIN pg_namespace n ON n.oid = cls.relnamespace
        LEFT JOIN pg_constraint con ON con.conindid = i.indexrelid
        WHERE n.nspname = 'public'
        ORDER BY cls.relname, ic.relname;
    """
    out = []
    for row in conn.execute(sql):
        table, name, defn, is_unique, is_primary, from_con = row
        if table in EXCLUDED_TABLES:
            continue
        out.append({
            "table": table,
            "name": name,
            "definition": defn,
            "is_unique": is_unique,
            "is_primary": is_primary,
            "from_constraint": from_con,
        })
    return out


# ---------------------------------------------------------------------------
# Render — Mermaid ERD
# ---------------------------------------------------------------------------


def build_mermaid(
    columns: dict[str, list[dict]],
    primary_keys: dict[str, list[str]],
    foreign_keys: list[dict],
    unique_constraints: list[dict],
) -> str:
    """Build a Mermaid erDiagram source string."""

    # Set of (table, column) pairs belonging to FK.
    fk_cols: set[tuple[str, str]] = set()
    for fk in foreign_keys:
        for c in fk["columns"]:
            fk_cols.add((fk["table"], c))

    # Single-column UNIQUE constraints -> UK marker.
    uk_cols: set[tuple[str, str]] = set()
    for uc in unique_constraints:
        # Definition is e.g. "UNIQUE (cik)" or "UNIQUE (a, b, c)".
        defn = uc["definition"]
        if defn.upper().startswith("UNIQUE ("):
            inside = defn[defn.index("(") + 1 : defn.rindex(")")]
            cols = [c.strip() for c in inside.split(",")]
            if len(cols) == 1:
                uk_cols.add((uc["table"], cols[0]))

    pk_cols: set[tuple[str, str]] = set()
    for t, cols in primary_keys.items():
        for c in cols:
            pk_cols.add((t, c))

    lines: list[str] = ["erDiagram"]

    # Relationships
    for fk in foreign_keys:
        left = fk["foreign_table"].upper()
        right = fk["table"].upper()
        # Cardinality: "one" on parent side if child FK columns are all NOT NULL,
        # else "zero or one". Right side is always "zero or many" for standard FKs.
        child_notnull_all = True
        child_cols = set(fk["columns"])
        for col in columns.get(fk["table"], []):
            if col["name"] in child_cols and not col["not_null"]:
                child_notnull_all = False
                break
        left_card = "||" if child_notnull_all else "|o"
        rel = f"  {left} {left_card}--o{{ {right} : \"{fk['name']}\""
        lines.append(rel)

    # Entities
    for table in sorted(columns.keys()):
        lines.append(f"  {table.upper()} {{")
        for col in columns[table]:
            markers = []
            pk = (table, col["name"]) in pk_cols
            fk = (table, col["name"]) in fk_cols
            uk = (table, col["name"]) in uk_cols and not pk
            if pk:
                markers.append("PK")
            if fk:
                markers.append("FK")
            if uk:
                markers.append("UK")
            marker_str = " ".join(markers) if markers else ""
            # Comment: "NOT NULL" or "nullable", with default if any.
            note_parts = []
            note_parts.append("NOT NULL" if col["not_null"] else "nullable")
            if col["default"]:
                note_parts.append(f"default {col['default']}")
            note = ", ".join(note_parts)
            # Mermaid escapes inside the quoted comment: strip any embedded quotes.
            note = note.replace('"', "'")
            dtype = mermaid_type(col["type"])
            if marker_str:
                lines.append(f"    {dtype} {col['name']} {marker_str} \"{note}\"")
            else:
                lines.append(f"    {dtype} {col['name']} \"{note}\"")
        lines.append("  }")

    return "\n".join(lines)


def mermaid_type(pg_type: str) -> str:
    """Mermaid ER identifiers cannot contain spaces or parens. Flatten."""
    t = pg_type.replace(" ", "_").replace("(", "_").replace(")", "")
    t = t.replace(",", "_")
    return t


# ---------------------------------------------------------------------------
# Render — HTML
# ---------------------------------------------------------------------------


def render_table_card(
    table: str,
    cols: list[dict],
    primary_keys: dict[str, list[str]],
    foreign_keys: list[dict],
    unique_constraints: list[dict],
    check_constraints: list[dict],
    indexes: list[dict],
) -> str:
    pk_set = set(primary_keys.get(table, []))
    table_fks = [fk for fk in foreign_keys if fk["table"] == table]
    table_uks = [uc for uc in unique_constraints if uc["table"] == table]
    table_checks = [cc for cc in check_constraints if cc["table"] == table]
    table_indexes = [ix for ix in indexes if ix["table"] == table]

    fk_cols: dict[str, dict] = {}
    for fk in table_fks:
        if len(fk["columns"]) == 1:
            fk_cols[fk["columns"][0]] = fk

    rows_html = []
    for col in cols:
        key_badges = []
        if col["name"] in pk_set:
            key_badges.append('<span class="badge pk">PK</span>')
        if col["name"] in fk_cols:
            fk = fk_cols[col["name"]]
            key_badges.append(
                f'<span class="badge fk">FK→{html.escape(fk["foreign_table"])}.{html.escape(fk["foreign_columns"][0])}</span>'
            )
        null_label = "NOT NULL" if col["not_null"] else "nullable"
        null_class = "notnull" if col["not_null"] else "nullable"
        default = html.escape(col["default"] or "")
        rows_html.append(f"""
            <tr>
                <td class="col-name">{html.escape(col['name'])}</td>
                <td class="col-type"><code>{html.escape(col['type'])}</code></td>
                <td class="col-null"><span class="null {null_class}">{null_label}</span></td>
                <td class="col-default"><code>{default}</code></td>
                <td class="col-keys">{' '.join(key_badges)}</td>
            </tr>
        """)

    # Composite / non-single-col UNIQUE constraints go in the "rules" block
    uk_html = ""
    if table_uks:
        uk_html = "<h4>Unique constraints</h4><ul>"
        for uc in table_uks:
            uk_html += f"<li><code>{html.escape(uc['definition'])}</code> <span class='cname'>({html.escape(uc['name'])})</span></li>"
        uk_html += "</ul>"

    # FKs (full detail with ON DELETE)
    fk_html = ""
    if table_fks:
        fk_html = "<h4>Foreign keys</h4><ul>"
        for fk in table_fks:
            cols_str = ", ".join(fk["columns"])
            fcols_str = ", ".join(fk["foreign_columns"])
            fk_html += (
                f"<li><code>({html.escape(cols_str)})</code> → "
                f"<code>{html.escape(fk['foreign_table'])}({html.escape(fcols_str)})</code> "
                f"<span class='badge on-delete'>ON DELETE {html.escape(fk['on_delete'])}</span> "
                f"<span class='cname'>({html.escape(fk['name'])})</span></li>"
            )
        fk_html += "</ul>"

    check_html = ""
    if table_checks:
        check_html = "<h4>Check constraints</h4><ul>"
        for cc in table_checks:
            check_html += (
                f"<li><code class='check-def'>{html.escape(cc['definition'])}</code> "
                f"<span class='cname'>({html.escape(cc['name'])})</span></li>"
            )
        check_html += "</ul>"

    index_html = ""
    if table_indexes:
        index_html = "<h4>Indexes</h4><ul>"
        for ix in table_indexes:
            tags = []
            if ix["is_primary"]:
                tags.append('<span class="badge pk">PK</span>')
            elif ix["is_unique"]:
                tags.append('<span class="badge uk">UNIQUE</span>')
            if ix["from_constraint"] and not ix["is_primary"]:
                tags.append('<span class="badge cons">from constraint</span>')
            if "WHERE" in ix["definition"].upper():
                tags.append('<span class="badge partial">partial</span>')
            tag_str = " ".join(tags)
            index_html += (
                f"<li><b>{html.escape(ix['name'])}</b> {tag_str}<br>"
                f"<code class='index-def'>{html.escape(ix['definition'])}</code></li>"
            )
        index_html += "</ul>"

    return f"""
    <section class="table-card" id="tbl-{html.escape(table)}">
        <h3>{html.escape(table)}</h3>
        <table class="cols">
            <thead>
                <tr><th>Column</th><th>Type</th><th>Null</th><th>Default</th><th>Keys</th></tr>
            </thead>
            <tbody>
                {''.join(rows_html)}
            </tbody>
        </table>
        {fk_html}
        {uk_html}
        {check_html}
        {index_html}
    </section>
    """


def render_html(mermaid_src: str, table_cards: str, table_names: list[str]) -> str:
    nav = " · ".join(
        f"<a href='#tbl-{html.escape(t)}'>{html.escape(t)}</a>" for t in table_names
    )
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Arrow schema — live view</title>
<style>
    :root {{
        --fg: #3d3d3a;
        --bg: #faf9f5;
        --muted: #73726c;
        --line: #d8d6cd;
        --card-bg: #ffffff;
        --accent: #b85c1c;
        --pk: #3f7a3f;
        --fk: #2b5c8c;
        --uk: #8c2b6f;
        --cons: #8a7a2a;
        --partial: #6b3fa8;
        --ondelete: #555;
    }}
    @media (prefers-color-scheme: dark) {{
        :root {{
            --fg: #c2c0b6;
            --bg: #1d1d1a;
            --muted: #9c9a92;
            --line: #3a3a35;
            --card-bg: #26262200;
            --accent: #d98642;
            --pk: #7ec07e;
            --fk: #5e9ed6;
            --uk: #d87ab8;
            --cons: #ccb85e;
            --partial: #b091dd;
            --ondelete: #9c9a92;
        }}
    }}
    * {{ box-sizing: border-box; }}
    body {{
        font-family: "Anthropic Sans", "SF Pro Text", -apple-system, system-ui, sans-serif;
        color: var(--fg);
        background: var(--bg);
        margin: 0;
        padding: 2rem;
        max-width: 1400px;
        margin-inline: auto;
        font-size: 14px;
        line-height: 1.55;
    }}
    h1 {{ margin-top: 0; }}
    h2 {{ border-bottom: 1px solid var(--line); padding-bottom: 0.25rem; margin-top: 2.5rem; }}
    h3 {{ margin-bottom: 0.5rem; color: var(--accent); }}
    h4 {{ margin: 1rem 0 0.25rem; font-size: 13px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.05em; }}
    code {{ font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 12.5px; }}
    .legend {{ display: flex; flex-wrap: wrap; gap: 0.75rem; margin-block: 1rem; }}
    .legend span {{ font-size: 12px; }}
    .badge {{
        display: inline-block;
        padding: 1px 6px;
        border-radius: 3px;
        font-size: 10.5px;
        font-weight: 600;
        letter-spacing: 0.05em;
        border: 1px solid currentColor;
    }}
    .badge.pk {{ color: var(--pk); }}
    .badge.fk {{ color: var(--fk); }}
    .badge.uk {{ color: var(--uk); }}
    .badge.cons {{ color: var(--cons); }}
    .badge.partial {{ color: var(--partial); }}
    .badge.on-delete {{ color: var(--ondelete); font-weight: 500; }}
    .null.notnull {{ color: var(--pk); font-weight: 500; }}
    .null.nullable {{ color: var(--muted); font-style: italic; }}
    nav.toc {{ margin-block: 1rem; font-size: 12.5px; color: var(--muted); }}
    nav.toc a {{ color: var(--accent); text-decoration: none; }}
    nav.toc a:hover {{ text-decoration: underline; }}
    .cards {{ display: flex; flex-direction: column; gap: 2rem; }}
    .table-card {{
        background: var(--card-bg);
        border: 1px solid var(--line);
        border-radius: 8px;
        padding: 1rem 1.25rem 0.5rem;
    }}
    table.cols {{
        border-collapse: collapse;
        width: 100%;
        margin-block: 0.5rem;
    }}
    table.cols th, table.cols td {{
        text-align: left;
        padding: 4px 8px;
        border-bottom: 1px solid var(--line);
        vertical-align: top;
    }}
    table.cols th {{ font-size: 11.5px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.05em; font-weight: 500; }}
    .col-name {{ font-weight: 600; }}
    .col-default code {{ color: var(--muted); font-size: 11.5px; }}
    ul {{ padding-left: 1.2rem; margin-block: 0.25rem; }}
    ul li {{ margin-block: 0.2rem; }}
    .cname {{ font-size: 11px; color: var(--muted); }}
    code.check-def, code.index-def {{ white-space: pre-wrap; word-break: break-word; }}
    #erd-shell {{
        border: 1px solid var(--line);
        border-radius: 10px;
        background: color-mix(in srgb, var(--card-bg) 92%, transparent);
        overflow: hidden;
    }}
    .erd-toolbar {{
        display: flex;
        align-items: center;
        gap: 0.5rem;
        padding: 0.75rem 0.9rem;
        border-bottom: 1px solid var(--line);
        background: color-mix(in srgb, var(--bg) 82%, var(--card-bg));
    }}
    .erd-toolbar button {{
        appearance: none;
        border: 1px solid var(--line);
        background: var(--card-bg);
        color: var(--fg);
        border-radius: 6px;
        padding: 0.3rem 0.55rem;
        font: inherit;
        cursor: pointer;
    }}
    .erd-toolbar button:hover {{ border-color: var(--accent); color: var(--accent); }}
    .erd-toolbar .spacer {{ flex: 1; }}
    .erd-hint {{ color: var(--muted); font-size: 12px; }}
    #erd-viewport {{
        position: relative;
        height: min(78vh, 980px);
        overflow: hidden;
        cursor: grab;
        touch-action: none;
        background:
          radial-gradient(circle at 1px 1px, color-mix(in srgb, var(--line) 55%, transparent) 1px, transparent 0);
        background-size: 18px 18px;
    }}
    #erd-viewport.is-dragging {{ cursor: grabbing; }}
    #erd {{
        width: 100%;
        height: 100%;
        position: relative;
    }}
    #erd svg.erDiagram {{
        display: block;
        max-width: none;
        height: auto;
        will-change: transform;
        transform-origin: 0 0;
        user-select: none;
    }}
    #erd svg.erDiagram .divider path {{ stroke-opacity: 0.5; }}
    #erd svg.erDiagram .row-rect-odd path,
    #erd svg.erDiagram .row-rect-odd rect,
    #erd svg.erDiagram .row-rect-even path,
    #erd svg.erDiagram .row-rect-even rect {{ stroke: none !important; }}
    #erd svg.erDiagram .node {{ cursor: pointer; }}
    #erd svg.erDiagram .node.is-active rect,
    #erd svg.erDiagram .node.is-active path {{
        stroke: var(--accent) !important;
        stroke-width: 2.5 !important;
    }}
    .table-card.flash {{
        border-color: var(--accent);
        box-shadow: 0 0 0 3px color-mix(in srgb, var(--accent) 18%, transparent);
    }}
    .meta {{ color: var(--muted); font-size: 12px; }}
    footer {{ margin-top: 3rem; color: var(--muted); font-size: 12px; border-top: 1px solid var(--line); padding-top: 1rem; }}
</style>
</head>
<body>

<h1>Arrow schema — live view</h1>
<p class="meta">Generated from the live database. Re-run <code>uv run scripts/gen_schema_viz.py</code> after any migration.</p>

<h2>Entity-relationship diagram</h2>
<div class="legend">
    <span><span class="badge pk">PK</span> primary key</span>
    <span><span class="badge fk">FK</span> foreign key</span>
    <span><span class="badge uk">UK</span> unique (single column)</span>
    <span>Cardinality: <code>||--o&#123;</code> = required parent, 0..N children · <code>|o--o&#123;</code> = optional parent (nullable FK), 0..N children</span>
</div>
<div id="erd-shell">
    <div class="erd-toolbar">
        <button type="button" id="zoom-in">+</button>
        <button type="button" id="zoom-out">−</button>
        <button type="button" id="zoom-reset">Reset</button>
        <div class="spacer"></div>
        <span class="erd-hint">Wheel to zoom. Drag to pan. Click a table to jump to its detail card.</span>
    </div>
    <div id="erd-viewport">
        <div id="erd"></div>
    </div>
</div>

<h2>Per-table detail</h2>
<nav class="toc">{nav}</nav>
<div class="cards">
    {table_cards}
</div>

<footer>
    Source: Postgres <code>public</code> schema, queried via <code>pg_catalog</code> + <code>information_schema</code>.
    Tables excluded from view: <code>schema_migrations</code> (migration tracker).
</footer>

<script type="module">
import mermaid from 'https://esm.sh/mermaid@11/dist/mermaid.esm.min.mjs';
const dark = matchMedia('(prefers-color-scheme: dark)').matches;
await document.fonts.ready;
mermaid.initialize({{
    startOnLoad: false,
    theme: 'base',
    fontFamily: '"Anthropic Sans", sans-serif',
    themeVariables: {{
        darkMode: dark,
        fontSize: '13px',
        fontFamily: '"Anthropic Sans", sans-serif',
        lineColor: dark ? '#9c9a92' : '#73726c',
        textColor: dark ? '#c2c0b6' : '#3d3d3a',
    }},
}});

const src = {mermaid_json_literal(mermaid_src)};
const {{ svg }} = await mermaid.render('erd-svg', src);
document.getElementById('erd').innerHTML = svg;
const tableNames = {mermaid_json_literal(table_names)};
const viewport = document.getElementById('erd-viewport');
const svgEl = document.querySelector('#erd svg.erDiagram');
const clamp = (value, min, max) => Math.min(max, Math.max(min, value));

// Round entity corners for a friendlier look
document.querySelectorAll('#erd svg.erDiagram .node').forEach(node => {{
    const firstPath = node.querySelector('path[d]');
    if (!firstPath) return;
    const d = firstPath.getAttribute('d');
    const nums = d.match(/-?[\\d.]+/g)?.map(Number);
    if (!nums || nums.length < 8) return;
    const xs = [nums[0], nums[2], nums[4], nums[6]];
    const ys = [nums[1], nums[3], nums[5], nums[7]];
    const x = Math.min(...xs), y = Math.min(...ys);
    const w = Math.max(...xs) - x, h = Math.max(...ys) - y;
    const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
    rect.setAttribute('x', x); rect.setAttribute('y', y);
    rect.setAttribute('width', w); rect.setAttribute('height', h);
    rect.setAttribute('rx', '8');
    for (const a of ['fill', 'stroke', 'stroke-width', 'class', 'style']) {{
        if (firstPath.hasAttribute(a)) rect.setAttribute(a, firstPath.getAttribute(a));
    }}
    firstPath.replaceWith(rect);
}});

const baseWidth = Number(svgEl.getAttribute('width')) || svgEl.viewBox.baseVal.width || svgEl.getBBox().width;
const baseHeight = Number(svgEl.getAttribute('height')) || svgEl.viewBox.baseVal.height || svgEl.getBBox().height;
let scale = 1;
let tx = 0;
let ty = 0;
let drag = null;

function setActiveNode(activeNode) {{
    document.querySelectorAll('#erd svg.erDiagram .node.is-active').forEach(node => {{
        node.classList.remove('is-active');
    }});
    if (activeNode) activeNode.classList.add('is-active');
}}

function flashCard(card) {{
    card.classList.add('flash');
    setTimeout(() => card.classList.remove('flash'), 1800);
}}

function applyTransform() {{
    svgEl.style.transform = `translate(${{tx}}px, ${{ty}}px) scale(${{scale}})`;
}}

function fitToViewport() {{
    const padding = 24;
    const vw = viewport.clientWidth - padding * 2;
    const vh = viewport.clientHeight - padding * 2;
    const fitScale = Math.min(vw / baseWidth, vh / baseHeight);
    scale = clamp(fitScale, 0.35, 1.1);
    tx = Math.max((viewport.clientWidth - baseWidth * scale) / 2, padding / 2);
    ty = Math.max((viewport.clientHeight - baseHeight * scale) / 2, padding / 2);
    applyTransform();
}}

function zoomAround(clientX, clientY, nextScale) {{
    const rect = viewport.getBoundingClientRect();
    const anchorX = clientX - rect.left;
    const anchorY = clientY - rect.top;
    const worldX = (anchorX - tx) / scale;
    const worldY = (anchorY - ty) / scale;
    scale = clamp(nextScale, 0.2, 3.5);
    tx = anchorX - worldX * scale;
    ty = anchorY - worldY * scale;
    applyTransform();
}}

document.getElementById('zoom-in').addEventListener('click', () => {{
    const rect = viewport.getBoundingClientRect();
    zoomAround(rect.left + rect.width / 2, rect.top + rect.height / 2, scale * 1.15);
}});
document.getElementById('zoom-out').addEventListener('click', () => {{
    const rect = viewport.getBoundingClientRect();
    zoomAround(rect.left + rect.width / 2, rect.top + rect.height / 2, scale / 1.15);
}});
document.getElementById('zoom-reset').addEventListener('click', fitToViewport);

viewport.addEventListener('wheel', event => {{
    event.preventDefault();
    const factor = event.deltaY < 0 ? 1.08 : 1 / 1.08;
    zoomAround(event.clientX, event.clientY, scale * factor);
}}, {{ passive: false }});

viewport.addEventListener('pointerdown', event => {{
    if (event.target.closest('.node')) return;
    drag = {{ x: event.clientX, y: event.clientY, tx, ty }};
    viewport.classList.add('is-dragging');
}});
window.addEventListener('pointermove', event => {{
    if (!drag) return;
    tx = drag.tx + (event.clientX - drag.x);
    ty = drag.ty + (event.clientY - drag.y);
    applyTransform();
}});
window.addEventListener('pointerup', () => {{
    drag = null;
    viewport.classList.remove('is-dragging');
}});

const normalizedTableNames = new Map(tableNames.map(name => [name.toUpperCase(), name]));
document.querySelectorAll('#erd svg.erDiagram .node').forEach(node => {{
    const labels = Array.from(node.querySelectorAll('text, tspan'))
        .map(el => el.textContent?.trim())
        .filter(Boolean);
    const table = labels.find(label => normalizedTableNames.has(label.toUpperCase()));
    if (!table) return;
    node.dataset.table = normalizedTableNames.get(table.toUpperCase());
    node.addEventListener('click', event => {{
        event.preventDefault();
        event.stopPropagation();
        const tableName = node.dataset.table;
        const card = document.getElementById(`tbl-${{tableName}}`);
        if (!card) return;
        setActiveNode(node);
        card.scrollIntoView({{ behavior: 'smooth', block: 'start' }});
        flashCard(card);
        history.replaceState(null, '', `#tbl-${{tableName}}`);
    }});
}});

window.addEventListener('resize', fitToViewport);
fitToViewport();
</script>

</body>
</html>
"""


def mermaid_json_literal(src: str) -> str:
    """JSON-encode the mermaid source so it embeds cleanly in JS."""
    import json
    return json.dumps(src)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    with get_conn() as conn:
        columns = fetch_columns(conn)
        primary_keys = fetch_primary_keys(conn)
        foreign_keys = fetch_foreign_keys(conn)
        unique_constraints = fetch_unique_constraints(conn)
        check_constraints = fetch_check_constraints(conn)
        indexes = fetch_indexes(conn)

    mermaid_src = build_mermaid(columns, primary_keys, foreign_keys, unique_constraints)

    table_names = sorted(columns.keys())
    cards_html = "\n".join(
        render_table_card(
            t,
            columns[t],
            primary_keys,
            foreign_keys,
            unique_constraints,
            check_constraints,
            indexes,
        )
        for t in table_names
    )

    html_out = "\n".join(
        line.rstrip() for line in render_html(mermaid_src, cards_html, table_names).splitlines()
    ) + "\n"
    OUT_PATH.write_text(html_out, encoding="utf-8")
    print(f"Wrote {OUT_PATH}")
    print(f"  tables: {len(table_names)}")
    print(f"  columns: {sum(len(v) for v in columns.values())}")
    print(f"  foreign keys: {len(foreign_keys)}")
    print(f"  check constraints: {len(check_constraints)}")
    print(f"  indexes: {len(indexes)}")


if __name__ == "__main__":
    main()
