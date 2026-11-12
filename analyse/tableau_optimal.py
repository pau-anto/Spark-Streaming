# ======================================================================
# TABLEAU DE L'ÉTUDE DE CHARGE ET COMBINAISON OPTIMALE
#   Lit output/perf.csv (écrit par le Consumer), regroupe par run
#   (identifiant b<batch>-i<interval>-p<parallelisme>[-m<max.files>][-cd]) et calcule :
#     - flux nominal / flux réel envoyé / débit réellement écoulé
#     - latence médiane et p95 (production -> écriture)
#     - dérive : médiane du dernier bloc / médiane du premier bloc
#   Une config est STABLE si la dérive reste < SEUIL_DERIVE ET si le débit écoulé
#   atteint au moins 90 % du flux envoyé (le Consumer suit le Producer).
#   Sans suffixe -m, le run est supposé fait avec max.files.per.trigger = 5.
#   Sortie : tableau affiché + output/tableau_optimal.csv
# Lancement depuis la racine Spark-Streaming/ :  python analyse/tableau_optimal.py
# ======================================================================

import pandas as pd

CSV = "output/perf.csv"
SORTIE = "output/tableau_optimal.csv"
BLOC = 100            # taille d'un bloc pour la médiane (dérive)
SEUIL_DERIVE = 1.5    # au-delà, la config est jugée instable
RUN_REGEX = r"^b(\d+)-i(\d+)-p(\d+)(?:-m(\d+))?(-cd)?$"

# ----------------------------------------------------------------------
# 1. Lecture : uniquement les images produites avec un identifiant de run
# ----------------------------------------------------------------------
df = pd.read_csv(CSV, sep=";", parse_dates=["heure_production", "heure_ecriture"])
df = df[df["image"].str.contains("__")].copy()
df["run"] = df["image"].str.split("__").str[0]
df = df[df["run"].str.match(RUN_REGEX)]          # ignore les lancements "manuel"
df["total_ms"] = df["temps_recuperation_ms"] + df["temps_ecriture_ms"]

if df.empty:
    raise SystemExit(f"Aucun run au format b<N>-i<ms>-p<N> trouvé dans {CSV}")


# ----------------------------------------------------------------------
# 2. Résumé d'un run
# ----------------------------------------------------------------------
def resume(g: pd.DataFrame) -> pd.Series:
    g = g.sort_values("heure_production").reset_index(drop=True)
    blocs = g["total_ms"].groupby(g.index // BLOC).median()
    duree_prod = (g["heure_production"].max() - g["heure_production"].min()).total_seconds()
    duree_totale = (g["heure_ecriture"].max() - g["heure_production"].min()).total_seconds()
    return pd.Series({
        "n": len(g),
        "flux_reel_img_s": len(g) / duree_prod if duree_prod > 0 else float("nan"),
        "debit_img_s": len(g) / duree_totale if duree_totale > 0 else float("nan"),
        "lat_mediane_ms": g["total_ms"].median(),
        "lat_p95_ms": g["total_ms"].quantile(0.95),
        "derive": blocs.iloc[-1] / blocs.iloc[0],
    })


res = df.groupby("run").apply(resume, include_groups=False)

# paramètres lus dans le nom du run
p = res.index.to_series().str.extract(RUN_REGEX)
res.insert(0, "batch", p[0].astype(int).values)
res.insert(1, "interval_ms", p[1].astype(int).values)
res.insert(2, "parallelisme", p[2].astype(int).values)
res.insert(3, "max_files", p[3].fillna("5").astype(int).values)
res.insert(4, "clean_source", p[4].notna().values)
res.insert(5, "flux_nominal_img_s", res["batch"] / (res["interval_ms"] / 1000))
res["stable"] = (res["derive"] < SEUIL_DERIVE) & (res["debit_img_s"] >= 0.9 * res["flux_reel_img_s"])

# ----------------------------------------------------------------------
# 3. Tableau complet + combinaison retenue
#    retenue = parmi les configs stables, plus haut flux, puis latence médiane la plus basse
# ----------------------------------------------------------------------
res = res.sort_values(["stable", "flux_nominal_img_s", "lat_mediane_ms"], ascending=[False, False, True])

pd.set_option("display.width", 250)
pd.set_option("display.max_columns", 30)
print(res.round(2).to_string())
res.round(3).to_csv(SORTIE, sep=";")
print(f"\nTableau enregistré : {SORTIE}")

# comparaison des réglages du Consumer à flux égal
if res[["max_files", "clean_source"]].drop_duplicates().shape[0] > 1:
    print("\nLatence médiane (s) par flux nominal (lignes) et réglage du Consumer (colonnes) :")
    print((res.pivot_table(index="flux_nominal_img_s", columns=["max_files", "clean_source"],
                           values="lat_mediane_ms", aggfunc="median") / 1000).round(1).to_string())
    print("\nConfig stable ? :")
    print(res.pivot_table(index="flux_nominal_img_s", columns=["max_files", "clean_source"],
                          values="stable", aggfunc="max").to_string())

stables = res[res["stable"]]
if stables.empty:
    print(f"\nAucune config stable : baisse le flux ou change le réglage du Consumer (max.files.per.trigger).")
else:
    best = stables.iloc[0]
    print(f"\nCombinaison retenue : {stables.index[0]}")
    print(f"  batch.size            = {int(best['batch'])}")
    print(f"  interval.ms           = {int(best['interval_ms'])}")
    print(f"  parallelism           = {int(best['parallelisme'])}")
    print(f"  max.files.per.trigger = {int(best['max_files'])} (Consumer)")
    print(f"  cleanSource           = {'delete' if best['clean_source'] else 'off'} (Consumer)")
    print(f"  flux {best['flux_nominal_img_s']:.2f} img/s | débit {best['debit_img_s']:.2f} img/s"
          f" | médiane {best['lat_mediane_ms']:.0f} ms | p95 {best['lat_p95_ms']:.0f} ms"
          f" | dérive {best['derive']:.2f}")

    # fenêtre de 100 images qui avance de 1 ou de 100, pour la config retenue
    g = df[df["run"] == stables.index[0]].sort_values("heure_production")
    med_pas1 = g["total_ms"].rolling(BLOC, step=1).median().dropna()
    med_pas100 = g["total_ms"].rolling(BLOC, step=BLOC).median().dropna()
    print(f"\nFenêtre de {BLOC} images, pas 1   : {len(med_pas1)} points, "
          f"médiane min/max {med_pas1.min():.0f}/{med_pas1.max():.0f} ms")
    print(f"Fenêtre de {BLOC} images, pas {BLOC} : {len(med_pas100)} points")