"""Biểu đồ từ metrics/phase5_metrics.json (matplotlib, PNG).

Màu theo thực thể, cố định: ALS = xanh dương, Popularity = cam, Hybrid = xanh ngọc (bảng màu tham chiếu
đã kiểm định cho 3 slot đầu). Mọi cột đều có nhãn giá trị trực tiếp nên không dựa vào màu đơn thuần.
"""
from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker  # noqa: E402,F401

SURFACE, INK, MUTED, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
COLORS = {"als": "#2a78d6", "popularity": "#eb6834", "hybrid_als_popularity": "#1baf7a",
          "item_mean": "#eda100", "global_mean": "#6250d6"}
LABELS = {"als": "ALS", "popularity": "Popularity", "hybrid_als_popularity": "Hybrid (ALS + Popularity)",
          "item_mean": "TB theo item", "global_mean": "TB toàn cục"}


def _style(ax):
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=MUTED, labelsize=9)
    ax.yaxis.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def _fig(*args, **kwargs):
    fig, axes = plt.subplots(*args, **kwargs)
    fig.patch.set_facecolor(SURFACE)
    return fig, axes


def _label(ax, bars, fmt):
    for bar in bars:
        h = bar.get_height()
        ax.annotate(fmt(h), (bar.get_x() + bar.get_width() / 2, h), ha="center", va="bottom",
                    fontsize=8.5, color=INK, xytext=(0, 2), textcoords="offset points")


def chart_split(metrics: dict, path: Path):
    parts = metrics["split"]["events_per_partition"]
    names = ["Train\n(≤50%)", "Validation 1\n(50–70%)", "Validation 2\n(70–85%)", "Test\n(>85%)"]
    values = list(parts.values())
    fig, ax = _fig(figsize=(7.2, 3.6))
    _style(ax)
    bars = ax.bar(names, values, color="#2a78d6", width=0.55)
    _label(ax, bars, lambda v: f"{v:,.0f}".replace(",", "."))
    ax.set_ylabel("Số sự kiện", color=MUTED)
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}".replace(",", ".")))
    ax.set_title("Chia dữ liệu theo thời gian (trước khi gộp user–item)", color=INK, fontsize=11, loc="left")
    ax.set_ylim(0, max(values) * 1.15)
    fig.tight_layout()
    fig.savefig(path, dpi=170, facecolor=SURFACE)
    plt.close(fig)


def chart_ranking(metrics: dict, path: Path, k: int):
    held = metrics["heldout_test"]
    models = ["als", "popularity", "hybrid_als_popularity"]
    keys = [("precision_at_k", f"Precision@{k}"), ("recall_at_k", f"Recall@{k}"), ("ndcg_at_k", f"NDCG@{k}")]
    tiers = [("warm", "User đã có lịch sử (warm)"), ("overall_weighted", "Toàn bộ user mục tiêu (gồm cold)")]
    fig, axes = _fig(2, 3, figsize=(10.5, 6.2))
    for row, (tier, tier_name) in enumerate(tiers):
        for col, (key, title) in enumerate(keys):
            ax = axes[row][col]
            _style(ax)
            values = [held[m][tier][key] or 0.0 for m in models]
            bars = ax.bar(range(3), values, color=[COLORS[m] for m in models], width=0.62)
            _label(ax, bars, lambda v: f"{v:.4f}")
            ax.set_xticks(range(3))
            ax.set_xticklabels(["ALS", "Popularity", "Hybrid"], fontsize=9)
            ax.set_ylim(0, max(values + [1e-9]) * 1.22)
            ax.set_title(title, color=INK, fontsize=10, loc="left")
            if col == 0:
                ax.set_ylabel(tier_name, color=INK, fontsize=9)
    fig.suptitle("Held-out test: ALS vs Popularity vs Hybrid", color=INK, fontsize=12, x=0.02, ha="left")
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    fig.savefig(path, dpi=170, facecolor=SURFACE)
    plt.close(fig)


def chart_cv(metrics: dict, path: Path, k: int):
    candidates = metrics["cross_validation"]["candidates"]
    names = [f"rank {c['parameters']['rank']}\nreg {c['parameters']['reg_param']}\nα {c['parameters']['alpha']:g}"
             for c in candidates]
    selected = metrics["cross_validation"]["selected_parameters"]
    fig, ax = _fig(figsize=(max(6.5, 1.15 * len(candidates) + 2.5), 4.0))
    _style(ax)
    width = 0.36
    for fold_index, color in ((0, "#2a78d6"), (1, "#eb6834")):
        values = [c["folds"][fold_index]["als"]["warm"]["ndcg_at_k"] for c in candidates]
        xs = [i + (fold_index - 0.5) * width for i in range(len(candidates))]
        bars = ax.bar(xs, values, width=width * 0.92, color=color, label=f"Fold {fold_index + 1}")
        _label(ax, bars, lambda v: f"{v:.4f}")
    for i, c in enumerate(candidates):
        if c["parameters"] == selected:
            ax.annotate("được chọn", (i, 0), xytext=(0, -50), textcoords="offset points", ha="center",
                        fontsize=8.5, color=INK, annotation_clip=False)
    ax.set_xticks(range(len(candidates)))
    ax.set_xticklabels(names, fontsize=8.5)
    ax.set_ylabel(f"NDCG@{k} (user warm)", color=MUTED)
    ax.set_title("Cross-validation theo thời gian: NDCG@K của từng cấu hình ALS", color=INK, fontsize=11, loc="left")
    ax.set_ylim(0, ax.get_ylim()[1] * 1.18)
    ax.legend(frameon=False, fontsize=9, loc="upper right", ncol=2)
    fig.tight_layout()
    fig.savefig(path, dpi=170, facecolor=SURFACE)
    plt.close(fig)


def chart_rmse(metrics: dict, path: Path):
    rmse = metrics["rmse"]
    sel = rmse["selected"]
    grid = next(g for g in rmse["grid"] if g["reg_param"] == sel["reg_param"])
    groups = [("Fold 1", grid["folds"][0]["rmse"]), ("Fold 2", grid["folds"][1]["rmse"]),
              ("Test", rmse["heldout_test"]["rmse"])]
    series = [("als_clipped_1_5", "ALS explicit", "#2a78d6"), ("baseline_item_mean", "TB theo item", "#eda100"),
              ("baseline_global_mean", "TB toàn cục", "#6250d6")]
    fig, ax = _fig(figsize=(7.6, 4.0))
    _style(ax)
    width = 0.26
    top = 0
    for s_index, (key, label, color) in enumerate(series):
        values = [g[1][key] for g in groups]
        top = max(top, max(values))
        xs = [i + (s_index - 1) * width for i in range(len(groups))]
        bars = ax.bar(xs, values, width=width * 0.92, color=color, label=label)
        _label(ax, bars, lambda v: f"{v:.3f}")
    ax.set_xticks(range(len(groups)))
    ax.set_xticklabels([g[0] for g in groups])
    ax.set_ylim(0, top * 1.2)
    ax.set_ylabel("RMSE (thấp hơn là tốt hơn)", color=MUTED)
    ax.set_title("RMSE trên mức hành vi (1/3/5): ALS explicit so với baseline", color=INK, fontsize=11, loc="left")
    ax.legend(frameon=False, fontsize=9, loc="upper right", ncol=3)
    fig.tight_layout()
    fig.savefig(path, dpi=170, facecolor=SURFACE)
    plt.close(fig)


def make_all(metrics_path: str, out_dir: str) -> list[str]:
    metrics = json.loads(Path(metrics_path).read_text(encoding="utf-8"))
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    k = metrics["config"]["top_k"]
    chart_split(metrics, out / "01_split_timeline.png")
    chart_cv(metrics, out / "02_cross_validation.png", k)
    chart_ranking(metrics, out / "03_heldout_ranking.png", k)
    chart_rmse(metrics, out / "04_rmse.png")
    chart_pipeline(out / "00_pipeline.png")
    return sorted(p.name for p in out.glob("*.png"))




def chart_pipeline(path: Path):
    """Sơ đồ luồng Phase 5 (không phụ thuộc số liệu)."""
    from matplotlib.patches import FancyBboxPatch
    fig, ax = _fig(figsize=(10.5, 4.6))
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 46)
    ax.axis("off")

    def box(x, y, w, h, text, fill="#ffffff", edge="#2a78d6", size=9):
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.3,rounding_size=1.2",
                                    linewidth=1.4, edgecolor=edge, facecolor=fill))
        ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=size, color=INK)

    def arrow(x1, y1, x2, y2):
        ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                    arrowprops=dict(arrowstyle="-|>", color=MUTED, lw=1.4))

    box(1, 30, 17, 11, "Delta Lake\nSilver / Gold\n(Phase 4)", fill="#eaf2fc")
    box(23, 30, 19, 11, "Chia theo thời gian\n50% / 70% / 85%\n(trước khi gộp)", fill="#fdf0ea", edge="#eb6834")
    box(47, 30, 24, 11, "Spark ML Pipeline\nVectorAssembler\n→ ALS (implicit)", fill="#eaf2fc")
    box(76, 30, 22, 11, "Cross-validation\n2 fold thời gian\nchọn rank/reg/alpha", fill="#fdf0ea", edge="#eb6834")
    arrow(18.5, 35.5, 22.5, 35.5)
    arrow(42.5, 35.5, 46.5, 35.5)
    arrow(71.5, 35.5, 75.5, 35.5)

    box(1, 8, 22, 12, "Held-out test (>85%)\nALS vs Popularity\nvs Hybrid", fill="#e8f7f1", edge="#1baf7a")
    box(28, 8, 22, 12, "Metric xếp hạng\nPrecision / Recall /\nNDCG @10 · warm/cold", fill="#e8f7f1", edge="#1baf7a")
    box(55, 8, 20, 12, "RMSE phụ trợ\nALS explicit vs\nbaseline", fill="#e8f7f1", edge="#1baf7a")
    box(80, 8, 18, 12, "Fit lại toàn bộ\n→ model phục vụ\n→ Redis / API", fill="#eaf2fc")
    arrow(87, 29.5, 87, 24)
    ax.plot([87, 12], [24, 24], color=MUTED, lw=1.4)
    arrow(12, 24, 12, 20.5)
    arrow(23.5, 14, 27.5, 14)
    arrow(50.5, 14, 54.5, 14)
    arrow(75.5, 14, 79.5, 14)
    ax.set_title("Phase 5 – Tầng phân tích / ML (task 16, 17, 18)", color=INK, fontsize=12, loc="left")
    fig.tight_layout()
    fig.savefig(path, dpi=170, facecolor=SURFACE)
    plt.close(fig)


if __name__ == "__main__":
    import sys
    print(make_all(sys.argv[1], sys.argv[2]))
