import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}

import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object ImageProducer {

  // Timestamp
  val timestampFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  def log(message: String): Unit = {
    val timestamp =
      LocalDateTime.now().format(timestampFormatter)

    println(s"[$timestamp] $message")
  }

  def main(args: Array[String]): Unit = {

    // ----------------------------------------------------------
    // Configuration (producer/src/main/resources/application.conf)
    // Chaque reglage peut etre surcharge par variable d'environnement
    // (BATCH_SIZE, INTERVAL_MS, PARALLELISM, RUN_ID) : voir sweep.py
    // ----------------------------------------------------------

    val config = ConfigFactory.load()

    val sourceDir =
      new Path(
        config.getString("producer.source.dir")
      ).toUri.toString

    val destinationDir =
      new Path(
        config.getString("producer.destination.dir")
      ).toUri.toString

    val batchSize =
      config.getInt("producer.batch.size")

    // Periode entre le DEBUT de deux batches (cadence fixe)
    val intervalMs =
      config.getLong("producer.interval.ms")

    // Nombre de copies simultanees (partitions + threads Spark)
    val parallelism =
      config.getInt("producer.parallelism")

    // Identifiant du test : prefixe des noms de fichiers dans dest/
    val runId =
      config.getString("producer.run.id")

    val deleteSource =
      config.getBoolean("producer.delete.source")

    val overwrite =
      config.getBoolean("producer.overwrite")

    require(batchSize > 0, "producer.batch.size doit etre > 0")
    require(parallelism > 0, "producer.parallelism doit etre > 0")
    require(
      !runId.contains("__"),
      "producer.run.id ne doit pas contenir '__' (separateur lu par le Consumer)"
    )

    // ----------------------------------------------------------
    // Spark
    // ----------------------------------------------------------

    val spark =
      SparkSession.builder()
        .appName("Image Producer")
        .master(s"local[$parallelism]") // p=1 : copie vraiment sequentielle
        .config("spark.ui.enabled", "false")
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val sc =
      spark.sparkContext

    log(
      s"Producer demarre | run=$runId | batch=$batchSize | " +
      s"interval=${intervalMs} ms | parallelisme=$parallelism"
    )

    // ----------------------------------------------------------
    // Decouverte des fichiers
    // ----------------------------------------------------------

    // sort : meme ordre d'envoi a chaque test, donc des runs comparables
    val files =
      sc.binaryFiles(sourceDir)
        .keys
        .filter(isImageFile)
        .collect()
        .sorted

    log(s"${files.length} image(s) trouvee(s)")

    if (files.isEmpty) {
      log("Aucune image a copier")
      spark.stop()
      return
    }

    // ----------------------------------------------------------
    // Creation des batches
    // ----------------------------------------------------------

    val batches =
      files.toSeq.grouped(batchSize).toSeq

    // Compteurs (accumulateurs : fiables car mis a jour dans une action)
    val copiedCount = sc.longAccumulator("copied")
    val failedCount = sc.longAccumulator("failed")

    val runStart = System.nanoTime()

    // ----------------------------------------------------------
    // Traitement des batches
    // ----------------------------------------------------------

    batches.zipWithIndex.foreach { case (batch, batchIndex) =>

      val batchStart = System.nanoTime()

      log(
        s"Debut batch ${batchIndex + 1}/${batches.length} - ${batch.size} image(s)"
      )

      // RDD du batch, reparti sur `parallelism` partitions
      val batchRDD =
        sc.parallelize(batch, parallelism)

      // --------------------------------------------------------
      // Copie en parallele
      // --------------------------------------------------------

      batchRDD.foreachPartition { partition =>

        val conf =
          new Configuration()

        partition.foreach { file =>

          val sourcePath = new Path(file)

          val fileName = sourcePath.getName

          val className = sourcePath.getParent.getName

          // run__classe_fichier.jpg : le Consumer retire le prefixe "run__"
          val destName = s"${runId}__${className}_$fileName"

          val destinationPath = new Path(s"$destinationDir/$destName")

          // Fichier cache : ignore par Spark tant que la copie n'est pas finie
          val tmpPath = new Path(s"$destinationDir/.$destName.tmp")

          // ----------------------------------------------------
          // Copie d'un fichier : .tmp puis rename (atomique)
          // Pas de FileSystem.close() : l'instance est partagee
          // par tous les threads de la JVM.
          // ----------------------------------------------------

          try {

            log(s"Copie de $fileName...")

            val startTime =
              System.nanoTime()

            val sourceFS =
              sourcePath.getFileSystem(conf)

            val destinationFS =
              destinationPath.getFileSystem(conf)

            // La source n'est supprimee qu'apres le rename reussi
            val copied =
              FileUtil.copy(
                sourceFS,
                sourcePath,
                destinationFS,
                tmpPath,
                false,
                overwrite,
                conf
              )

            val published =
              copied && destinationFS.rename(tmpPath, destinationPath)

            if (!published) {
              destinationFS.delete(tmpPath, false)
            } else if (deleteSource) {
              sourceFS.delete(sourcePath, false)
            }

            val durationMs =
              (System.nanoTime() - startTime) / 1000000

            if (published) {

              copiedCount.add(1)

              log(
                s"SUCCES : $fileName | ${durationMs} ms"
              )

            } else {

              failedCount.add(1)

              log(
                s"ECHEC : $fileName | ${durationMs} ms"
              )
            }

          } catch {

            case e: Exception =>

              failedCount.add(1)

              log(
                s"ERREUR : $fileName | ${e.getClass.getSimpleName} | ${e.getMessage}"
              )
          }
        }
      }

      val elapsedMs =
        (System.nanoTime() - batchStart) / 1000000

      log(
        s"Fin batch ${batchIndex + 1}/${batches.length} en ${elapsedMs} ms"
      )

      // --------------------------------------------------------
      // Cadence fixe : on attend (interval - duree de la copie),
      // donc flux = batch.size / (interval.ms / 1000) images/s
      // tant que la copie dure moins que l'intervalle.
      // (Ancien comportement : Thread.sleep(intervalMs) apres la copie.)
      // --------------------------------------------------------

      if (batchIndex < batches.length - 1) {

        val waitMs =
          math.max(0L, intervalMs - elapsedMs)

        if (elapsedMs > intervalMs) {
          log(
            s"ATTENTION : copie (${elapsedMs} ms) plus longue que l'intervalle " +
            s"(${intervalMs} ms), cadence non tenue"
          )
        } else {
          log(s"Attente de ${waitMs} ms")
        }

        Thread.sleep(waitMs)
      }
    }

    // ----------------------------------------------------------
    // Fin
    // ----------------------------------------------------------

    val totalSeconds =
      (System.nanoTime() - runStart) / 1e9

    log(
      s"Run $runId : ${copiedCount.value} copiee(s), " +
      s"${failedCount.value} echec(s) en ${"%.1f".format(totalSeconds)} s"
    )

    log("Toutes les images ont ete traitees")

    spark.stop()

    log("Producer termine")
  }

  // Verification des fichiers images
  def isImageFile(path: String): Boolean = {

  val lowerPath =
    path.toLowerCase

  lowerPath.endsWith(".jpg") ||
  lowerPath.endsWith(".jpeg")
}
}