import csv
import os
from collections import defaultdict
import matplotlib.pyplot as plt

TOP_N = 6
OUTPUT_DIR = "public/images"

# --- Load data ---
rows = []
with open("top_hashtags.csv", "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        rows.append({
            "hashtag": row["hashtag"],
            "category": row["category"],
            "count": int(row["total_count"])
        })

by_category = defaultdict(list)
for r in rows:
    by_category[r["category"]].append(r)

for cat in by_category:
    by_category[cat] = sorted(by_category[cat], key=lambda x: x["count"], reverse=True)[:TOP_N]

os.makedirs(OUTPUT_DIR, exist_ok=True)

BUBBLE_RADIUS = 0.85
bubbles_per_row = 3
bx_spacing = BUBBLE_RADIUS * 2.3
by_spacing = BUBBLE_RADIUS * 2.3

for cat, items in by_category.items():
    if not items:
        continue

    max_count = max(item["count"] for item in items)
    min_count = min(item["count"] for item in items)

    base_color = (0.16, 0.35, 0.60)  # single base hue per image; darkness = frequency
    def shade_for_count(count):
        frac = (count - min_count) / max(1, (max_count - min_count))
        darkness = 0.5 + 0.5 * frac
        r, g, b = base_color
        return (r * darkness, g * darkness, b * darkness, 0.9)

    n_items = len(items)
    item_rows = -(-n_items // bubbles_per_row)
    block_w = min(n_items, bubbles_per_row) * bx_spacing
    block_h = item_rows * by_spacing

    fig_w = max(6, block_w + 2)
    fig_h = block_h + 2.5
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))

    start_x = -block_w / 2 + bx_spacing / 2
    start_y = block_h / 2 - by_spacing / 2

    for i, item in enumerate(items):
        r_i = i // bubbles_per_row
        c_i = i % bubbles_per_row
        bx = start_x + c_i * bx_spacing
        by = start_y - r_i * by_spacing

        color = shade_for_count(item["count"])
        circle = plt.Circle((bx, by), BUBBLE_RADIUS, color=color, edgecolor="black", linewidth=0.8, zorder=3)
        ax.add_patch(circle)

        text_len = len(item["hashtag"])
        fontsize = 12 if text_len <= 8 else max(8, 12 - 0.5 * (text_len - 8))
        ax.text(bx, by, item["hashtag"], fontsize=fontsize, ha="center", va="center",
                 weight="bold", color="white", zorder=4)

    ax.set_xlim(-fig_w / 2, fig_w / 2)
    ax.set_ylim(-fig_h / 2, fig_h / 2)
    ax.set_aspect("equal")
    ax.axis("off")

    ax.set_title(f"Top Hashtags: {cat}\n(darker = higher post count)", fontsize=15, pad=15)

    plt.tight_layout()
    out_path = os.path.join(OUTPUT_DIR, f"{cat}.png")
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    print(f"Saved {out_path} ({n_items} hashtags)")

print("\nAll category images generated.")