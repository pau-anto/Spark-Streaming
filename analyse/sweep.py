# ======================================================================
# BALAYAGE DES CONFIGURATIONS (Producer) POUR UN RÉGLAGE DONNÉ DU CONSUMER
#   Pour chaque run (batch.size, interval.ms, parallelism) :
#     1. lance le Producer avec cette config (variables d'environnement)
#     2. attend que le Consumer ait écrit toutes les images du run
#     3. supprime les fichiers du run dans dest/ (chaque run repart d'un dossier léger)
#
#   Prérequis :
#     - le Consumer (mode predict) tourne DÉJÀ dans un autre terminal
#     - HADOOP_HOME et Path (C:\hadoop\bin) sont définis dans CE terminal
#     - MAX_FILES_PER_TRIGGER et CLEAN_SOURCE ont les MÊMES valeurs dans les deux
#       terminaux (le Consumer les lit ; ce script les met dans le nom du run)
#
# Lancement depuis la racine Spark-Streaming/ :
#   python analyse/sweep.py        # tous les runs
#   python analyse/sweep.py 1      # seulement le premier run (test rapide)
# ======================================================================

import os
import pathlib
import shutil
import subprocess
import sys
import time

PERF = pathlib.Path("output/perf.csv")   # = consumer.perf.csv
TEST_DIR = pathlib.Path("data/Test")     # = producer.source.dir (sans le /*)
DEST = pathlib.Path("dest")              # = producer.destination.dir
ATTENTE_MAX_SANS_PROGRES = 60            # secondes sans nouvelle image écrite -> on passe au run suivant
MARGE_ILLISIBLES = 5                     # images .jpg que le Consumer peut écarter (illisibles)

# Réglages du Consumer, repris dans le nom du run : b5-i2000-p4-m25-cd
MAX_FILES = os.environ.get("MAX_FILES_PER_TRIGGER", "5")
CLEAN = os.environ.get("CLEAN_SOURCE", "off")
TAG = f"-m{MAX_FILES}" + ("-cd" if CLEAN == "delete" else "")

# (batch.size, interval.ms, parallelism) ; flux nominal = batch / (interval / 1000) img/s
RUNS = [
    (5, 2000, 4),    # 2.5 img/s : sature le Consumer avec max.files.per.trigger = 5
    (5, 1000, 4),    # 5 img/s
    (10, 1000, 4),   # 10 img/s
]

N = sum(1 for p in TEST_DIR.rglob("*") if p.suffix.lower() in {".jpg", ".jpeg"})
ATTENDU = N - MARGE_ILLISIBLES
SBT = shutil.which("sbt") or "sbt"       # sbt.cmd sous Windows


def lignes_du_run(run: str) -> int:
    """Nombre d'images déjà écrites par le Consumer pour ce run."""
    if not PERF.exists():
        return 0
    with PERF.open(encoding="utf-8", errors="replace") as f:
        return sum(1 for ligne in f if ligne.startswith(run + "__"))


def purger_dest(run: str) -> None:
    """Supprime les images du run (et leurs .crc cachés) pour que dest/ reste léger."""
    if not DEST.exists():
        return
    for f in DEST.iterdir():
        if f.is_file() and (f.name.startswith(run + "__") or f.name.startswith("." + run + "__")):
            try:
                f.unlink()
            except OSError:
                pass  # fichier encore utilisé : sans importance


def main() -> None:
    if N == 0:
        raise SystemExit(f"Aucune image .jpg trouvée dans {TEST_DIR}")

    runs = RUNS[: int(sys.argv[1])] if len(sys.argv) > 1 else RUNS
    total_min = sum(N / (b / (ms / 1000)) for b, ms, _ in runs) / 60
    print(f"Réglages Consumer supposés : max.files.per.trigger={MAX_FILES}, cleanSource={CLEAN}")
    print(f"(vérifie qu'ils sont identiques dans le terminal du Consumer)")
    print(f"{N} image(s) de test, {len(runs)} run(s), durée minimale estimée ~{total_min:.0f} min")

    for b, ms, p in runs:
        run = f"b{b}-i{ms}-p{p}{TAG}"

        if lignes_du_run(run) > 0:
            print(f"[{run}] déjà présent dans {PERF}, on passe")
            continue

        flux = b / (ms / 1000)
        print(f"\n[{run}] flux nominal {flux:.1f} img/s, durée minimale ~{N / flux / 60:.1f} min")

        env = {
            **os.environ,
            "RUN_ID": run,
            "BATCH_SIZE": str(b),
            "INTERVAL_MS": str(ms),
            "PARALLELISM": str(p),
        }
        # Si le Consumer tourne, sbt demande "Create a new server? y/n" : répondre y (Entrée)
        subprocess.run(
            [SBT, "--batch", "-Dsbt.server.forcestart=true", "producer/run"],
            env=env, check=True,
        )

        # le Producer a fini : attendre que le Consumer ait tout écrit (file d'attente comprise)
        dernier_progres, precedent = time.time(), 0
        while lignes_du_run(run) < ATTENDU and time.time() - dernier_progres < ATTENTE_MAX_SANS_PROGRES:
            time.sleep(2)
            n = lignes_du_run(run)
            if n != precedent:
                precedent, dernier_progres = n, time.time()

        ecrites = lignes_du_run(run)
        etat = "OK" if ecrites >= ATTENDU else f"INCOMPLET ({ecrites}/{N})"
        print(f"[{run}] {etat} : {ecrites} image(s) écrite(s)")

        time.sleep(5)        # laisser le Consumer terminer son dernier micro-batch
        purger_dest(run)


if __name__ == "__main__":
    main()