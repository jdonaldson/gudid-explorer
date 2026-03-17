"""Generate Quarto site content from a .dyf file.

Reads scaffold, narration, and community data from an enriched .dyf file
and generates:
  - data/*.json files for the chat panel
  - categories/*.qmd pages (one per community)
  - categories/index.qmd listing page
  - Sidebar YAML for _quarto.yml (printed to stdout)
"""

import json
import re
import sys
from pathlib import Path

# Add project root so we can import dyf
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))


def slugify(name: str) -> str:
    """Convert a community name to a URL slug."""
    s = name.lower().strip()
    s = re.sub(r"[^a-z0-9\s-]", "", s)
    s = re.sub(r"[\s]+", "-", s)
    s = re.sub(r"-+", "-", s)
    return s.strip("-")


def generate_site(dyf_path_str: str) -> None:
    from dyf.enrich._scaffold import compute_scaffold, render_scaffold
    from dyf.lazy_index import LazyIndex, rewrite_lazy_index

    dyf_path = Path(dyf_path_str).resolve()
    site_dir = Path(__file__).resolve().parent.parent
    data_dir = site_dir / "data"
    cat_dir = site_dir / "categories"

    print(f"Reading {dyf_path}...")
    idx = LazyIndex(str(dyf_path))
    fields = idx.extract_all_fields()
    meta = fields["metadata"]

    # ── Extract scaffold data ──────────────────────────────────────────────
    scaffold_json_str = meta.get("llm_scaffold")
    if scaffold_json_str:
        scaffold_data = json.loads(scaffold_json_str)
    else:
        print("  No cached scaffold, computing...")
        scaffold_data = compute_scaffold(str(dyf_path))

    scaffold_text_str = meta.get("llm_scaffold_text")
    if not scaffold_text_str:
        scaffold_text_str = render_scaffold(scaffold_data)

    # ── Create site .dyf (keep embeddings for browser RAG search) ──────────
    viz_dyf_path = data_dir / "gudid_viz.dyf"
    print(f"  Creating site .dyf (with embeddings for RAG)...")
    rewrite_lazy_index(
        str(dyf_path),
        output_path=str(viz_dyf_path),
    )
    viz_size_mb = viz_dyf_path.stat().st_size / (1024 * 1024)
    print(f"  Wrote data/gudid_viz.dyf ({viz_size_mb:.1f} MB)")

    # ── Copy chat_idf_index.json if it exists nearby ───────────────────────
    idf_src = dyf_path.parent / "chat_idf_index.json"
    idf_dst = data_dir / "chat_idf_index.json"
    if idf_src.exists():
        idf_dst.write_bytes(idf_src.read_bytes())
        print(f"  Copied chat_idf_index.json")

    # ── Build communities.json ─────────────────────────────────────────────
    comms = scaffold_data["communities"]
    narration = json.loads(meta.get("tour_narration", "{}"))
    bundles = scaffold_data.get("bundles", [])
    similarity = scaffold_data.get("similarity", {})
    top_pairs = similarity.get("top_pairs", [])

    # Load IDF samples if available
    idf_index = None
    if idf_dst.exists():
        idf_index = json.loads(idf_dst.read_text())

    communities_data = {}
    for cid, info in comms.items():
        name = info["name"]
        size = info["size"]

        # Find bundle membership
        bundle_info = None
        for b in bundles:
            if cid in [str(m) for m in b["members"]]:
                bundle_info = {
                    "label": b["label"],
                    "siblings": [
                        {"id": str(m["id"]), "name": m["name"], "size": m["size"]}
                        for m in b["member_details"]
                        if str(m["id"]) != cid
                    ],
                }
                break

        # Similar categories
        similar = []
        for p in top_pairs:
            if str(p["a"]) == cid:
                similar.append(str(p["b"]))
            elif str(p["b"]) == cid:
                similar.append(str(p["a"]))
            if len(similar) >= 3:
                break

        # Description from narration
        desc = narration.get(cid, "")

        # Sample products from IDF index
        samples = []
        if idf_index and "samples" in idf_index:
            samples = idf_index["samples"].get(cid, [])[:10]

        communities_data[cid] = {
            "name": name,
            "size": size,
            "slug": slugify(name),
            "description": desc,
            "bundle": bundle_info,
            "similar": similar,
            "samples": samples,
        }

    (data_dir / "communities.json").write_text(json.dumps(communities_data, indent=2))
    print(f"  Wrote data/communities.json")

    # ── Generate category .qmd pages ──────────────────────────────────────
    for cid, cdata in communities_data.items():
        slug = cdata["slug"]
        name = cdata["name"]
        size = cdata["size"]
        desc = cdata["description"]
        bundle = cdata["bundle"]
        similar = cdata["similar"]
        samples = cdata["samples"]

        lines = [
            "---",
            f'title: "{name}"',
            "---",
            "",
        ]

        # Bundle callout
        if bundle:
            lines.append(f'::: {{.callout-note appearance="simple"}}')
            lines.append(
                f'Part of the **{bundle["label"]}** group '
                f"&middot; {size:,} items"
            )
            lines.append(":::")
            lines.append("")

        # Description
        if desc:
            lines.append(desc)
            lines.append("")

        # Sample products
        if samples:
            lines.append("## Sample Products")
            lines.append("")
            lines.append("| Product |")
            lines.append("|---------|")
            for s in samples:
                # Escape pipe characters in product names
                safe = s.replace("|", "\\|")
                lines.append(f"| {safe} |")
            lines.append("")

        # Related categories
        has_related = False
        if similar:
            similar_names = []
            for sid in similar:
                if sid in communities_data:
                    sname = communities_data[sid]["name"]
                    sslug = communities_data[sid]["slug"]
                    similar_names.append(f"[{sname}]({sslug}.qmd)")
            if similar_names:
                if not has_related:
                    lines.append("## Related Categories")
                    lines.append("")
                    has_related = True
                lines.append("**Most similar:**")
                lines.append("")
                for sn in similar_names:
                    lines.append(f"- {sn}")
                lines.append("")

        if bundle and bundle["siblings"]:
            if not has_related:
                lines.append("## Related Categories")
                lines.append("")
                has_related = True
            lines.append(f'**In the {bundle["label"]} group:**')
            lines.append("")
            for sib in bundle["siblings"]:
                sib_slug = slugify(sib["name"])
                lines.append(
                    f'- [{sib["name"]}]({sib_slug}.qmd) '
                    f'({sib["size"]:,} items)'
                )
            lines.append("")

        # Ask about button
        escaped_name = name.replace("'", "\\'")
        lines.append(
            f'<button class="btn btn-outline-primary btn-sm" '
            f"onclick=\"askChat('{escaped_name}')\">"
        )
        lines.append(f"  Ask about this category")
        lines.append("</button>")
        lines.append("")

        qmd_path = cat_dir / f"{slug}.qmd"
        qmd_path.write_text("\n".join(lines))

    print(f"  Generated {len(communities_data)} category pages")

    # ── Generate categories/index.qmd ─────────────────────────────────────
    index_lines = [
        "---",
        'title: "Categories"',
        "---",
        "",
        f"This dataset contains **{scaffold_data['n_items']:,}** items "
        f"organized into **{scaffold_data['n_communities']}** categories.",
        "",
    ]

    # Group by bundle
    for b in bundles:
        index_lines.append(f"### {b['label']}")
        index_lines.append(f"*{b['total_items']:,} items total*")
        index_lines.append("")
        for m in b["member_details"]:
            mid = str(m["id"])
            if mid in communities_data:
                mslug = communities_data[mid]["slug"]
                index_lines.append(
                    f"- [{m['name']}]({mslug}.qmd) ({m['size']:,} items)"
                )
        index_lines.append("")

    (cat_dir / "index.qmd").write_text("\n".join(index_lines))
    print(f"  Wrote categories/index.qmd")

    # ── Print sidebar YAML for _quarto.yml ────────────────────────────────
    print("\n── Sidebar YAML (paste into _quarto.yml) ──")
    print("    contents:")
    print("      - categories/index.qmd")
    for b in bundles:
        print(f'      - section: "{b["label"]}"')
        print("        contents:")
        for m in b["member_details"]:
            mid = str(m["id"])
            if mid in communities_data:
                mslug = communities_data[mid]["slug"]
                print(f"          - categories/{mslug}.qmd")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <path-to.dyf>")
        sys.exit(1)
    generate_site(sys.argv[1])
