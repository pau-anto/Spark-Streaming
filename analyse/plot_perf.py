# ======================================================================
# GRAPHIQUES DE PERFORMANCE (à partir de output/perf.csv)
#   perf.csv : une ligne par image -> image ; heure_production ; heure_ecriture
#   1. nombre d'images écrites en fonction du temps, une courbe par test
#   2. médiane du temps (écriture - production) par bloc, une courbe par test
# Sortie : output/perf.png
# Lancement depuis la racine Spark-Streaming/ :  python analyse/plot_perf.py
# ======================================================================

import matplotlib.pyplot as plt
import pandas as pd

CSV = "output/perf.csv"
BLOC = 100        # taille d'un bloc pour la médiane : 1 = chaque image, 100 = tous les 100 images
PAUSE_TEST = 30   # un trou de plus de 30 s entre deux images = un nouveau test

# couleurs des tests, toujours dans cet ordre (palette lisible par les daltoniens)
COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
GRID, TEXT = "#e3e2de", "#52514e"

# ----------------------------------------------------------------------
# 1. Lecture du CSV
# ----------------------------------------------------------------------
df = pd.read_csv(CSV, sep=";")
df["heure_production"] = pd.to_datetime(df["heure_production"])
df["heure_ecriture"] = pd.to_datetime(df["heure_ecriture"])
df = df.sort_values("heure_production").reset_index(drop=True)

# temps de chaque image : écriture - production, en ms
df["duree_ms"] = (df["heure_ecriture"] - df["heure_production"]).dt.total_seconds() * 1000

# ----------------------------------------------------------------------
# 2. Découper le fichier en tests : un nouveau test commence
#    quand plus de PAUSE_TEST secondes séparent deux images
# ----------------------------------------------------------------------
gap = df["heure_production"].diff().dt.total_seconds()
df["test"] = (gap.isna() | (gap > PAUSE_TEST)).cumsum()

tests = []
for test_id, t in df.groupby("test"):
    t = t.reset_index(drop=True)
    t0 = t["heure_production"].min()  # le chrono démarre à la production de la première image

    # avancement : nombre d'images écrites en fonction du temps
    written = (t["heure_ecriture"] - t0).dt.total_seconds().sort_values().reset_index(drop=True)

    # médiane par bloc de BLOC images
    t["bloc"] = t.index // BLOC
    blocs = t.groupby("bloc").agg(image_a=("duree_ms", "size"), mediane_ms=("duree_ms", "median"))
    blocs["image_a"] = blocs["image_a"].cumsum()

    start = t0.strftime("%H:%M:%S")
    tests.append((f"Test {test_id} ({start}, {len(t)} images)", written, blocs))

print(f"{len(tests)} test(s) trouvé(s) dans {CSV}")
for label, _, blocs in tests:
    print(f"\n{label}")
    print(blocs.to_string())


def style(ax, title, xlabel, ylabel):
    ax.set_title(title, loc="left", fontsize=11, color="#0b0b0b")
    ax.set_xlabel(xlabel, color=TEXT)
    ax.set_ylabel(ylabel, color=TEXT)
    ax.grid(color=GRID, linewidth=0.8)
    ax.tick_params(colors=TEXT)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)


fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5.5))

for i, (label, written, blocs) in enumerate(tests[: len(COLORS)]):
    # Graphique 1 : avancement. Plus la courbe monte vite, plus le débit est élevé.
    ax1.step(written, range(1, len(written) + 1), where="post", color=COLORS[i], linewidth=2, label=label)

    # Graphique 2 : médiane par bloc. Si elle grimpe au fil des blocs, le consumer est saturé.
    ax2.plot(blocs["image_a"], blocs["mediane_ms"], color=COLORS[i], linewidth=2,
             marker="o", markersize=4, label=label)

style(ax1, "Images écrites en fonction du temps", "Temps écoulé (s)", "Images écrites")
style(ax2, f"Médiane du temps écriture - production, par bloc de {BLOC}", "Images écrites", "Médiane (ms)")
ax1.legend(frameon=False, labelcolor=TEXT)
ax2.legend(frameon=False, labelcolor=TEXT)

fig.tight_layout()
fig.savefig("output/perf.png", dpi=150)
print("\nGraphique enregistré : output/perf.png")