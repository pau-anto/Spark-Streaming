import com.typesafe.config.ConfigFactory

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object ImageProducer {

  // ------------------------------------------------------------
  // Timestamp
  // ------------------------------------------------------------

  val timestampFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  def log(message: String): Unit = {
    val timestamp =
      LocalDateTime.now().format(timestampFormatter)

    println(s"[$timestamp] $message")
  }

  // ------------------------------------------------------------
  // Main
  // ------------------------------------------------------------

  def main(args: Array[String]): Unit = {

    log("========== DEMARRAGE DU PRODUCER ==========")

    // ----------------------------------------------------------
    // Configuration
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

    val intervalMs =
      config.getLong("producer.interval.ms")

    log(s"Source      : $sourceDir")
    log(s"Destination : $destinationDir")
    log(s"Batch size  : $batchSize")
    log(s"Intervalle  : ${intervalMs} ms")

    // ----------------------------------------------------------
    // SparkSession
    // ----------------------------------------------------------

    val spark =
      SparkSession.builder()
        .appName("Image Producer")
        .master("local[*]")
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    log("SparkSession demarree")

    // ----------------------------------------------------------
    // Recuperation du SparkContext
    // ----------------------------------------------------------

    val sc =
      spark.sparkContext

    // ----------------------------------------------------------
    // Decouverte des fichiers avec binaryFiles
    // ----------------------------------------------------------

    log("Recherche des fichiers dans la source...")

    val filesRDD =
      sc.binaryFiles(sourceDir)
        .keys
        .filter(isImageFile)

    // ----------------------------------------------------------
    // Recuperation des fichiers
    // ----------------------------------------------------------

    log("Recuperation de la liste des fichiers...")

    val files =
      filesRDD.collect()

    log(s"${files.length} fichier(s) image trouve(s)")

    if (files.isEmpty) {

      log("Aucune image trouvee")

      spark.stop()

      log("SparkSession arretee")
      log("========== FIN DU PRODUCER ==========")

      return
    }

    // ----------------------------------------------------------
    // Affichage des fichiers
    // ----------------------------------------------------------

    // *files.foreach { file =>
      // log(s" Fichier trouve: $file")
    // }

    // ----------------------------------------------------------
  // Creation des batches
  // ----------------------------------------------------------

  val batches =
    files.toSeq.grouped(batchSize).toSeq

  log(
    s"${batches.length} batch(s) cree(s)"
  )

  // ----------------------------------------------------------
  // Traitement des batches
  // ----------------------------------------------------------

  batches.zipWithIndex.foreach { case (batch, batchIndex) =>

    log(
      s"========== DEBUT BATCH ${batchIndex + 1}/${batches.length} =========="
    )

    log(
      s"${batch.size} fichier(s) dans ce batch"
    )

    // --------------------------------------------------------
    // Creation du RDD
    // --------------------------------------------------------

    val filesParallelized =
      sc.parallelize(batch)

    log(
      s"RDD cree avec ${filesParallelized.getNumPartitions} partition(s)"
    )

    // --------------------------------------------------------
    // Copie en parallele
    // --------------------------------------------------------

    filesParallelized.foreachPartition { partition =>

      val conf =
        new Configuration()

      val sourceFS =
        FileSystem.get(conf)

      val destinationFS =
        FileSystem.get(conf)

      try {

        // log("Nouvelle partition demarree")

        partition.foreach { file =>

          val sourcePath =
            new Path(file)

          val fileName =
            sourcePath.getName

          val destinationPath =
            new Path(
              s"$destinationDir/$fileName"
            )

          // --------------------------------------------------
          // Debut copie
          // --------------------------------------------------

          log(s"DEBUT COPIE : $fileName")

          log(s"Source      : $sourcePath")

          log(s"Destination : $destinationPath")

          val startTime =
            System.nanoTime()

          // --------------------------------------------------
          // Copie avec Hadoop
          // --------------------------------------------------

          val copied =
            FileUtil.copy(
              sourceFS,
              sourcePath,
              destinationFS,
              destinationPath,
              false,
              true,
              conf
            )

          // --------------------------------------------------
          // Temps de copie
          // --------------------------------------------------

          val durationMs =
            (System.nanoTime() - startTime) / 1000000

          if (copied) {

            log(
              s"SUCCES : $fileName | temps = ${durationMs} ms"
            )

          } else {

            log(
              s"ECHEC : $fileName | temps = ${durationMs} ms"
            )
          }
        }

        log("Partition terminee")

      } catch {

        case e: Exception =>

          log(
            s"ERREUR : ${e.getClass.getSimpleName}"
          )

          log(
            s"Message : ${e.getMessage}"
          )

          e.printStackTrace()

          throw e

      } finally {

        sourceFS.close()
        destinationFS.close()

        log("FileSystem ferme")
      }
    }

    log(
      s"========== FIN BATCH ${batchIndex + 1}/${batches.length} =========="
    )

    // ----------------------------------------------------------
    // Pause de 2 secondes
    // ----------------------------------------------------------

    if (batchIndex < batches.length - 1) {

      log(
        s"Attente de ${intervalMs} ms avant le prochain batch"
      )

      Thread.sleep(intervalMs)

      log("Fin de l'attente")
    }
  }

    // ----------------------------------------------------------
    // Fin
    // ----------------------------------------------------------

    log("Toutes les partitions ont termine")

    spark.stop()

    log("SparkSession arretee")

    log("========== FIN DU PRODUCER ==========")
  }

  // ------------------------------------------------------------
  // Verification des fichiers images
  // ------------------------------------------------------------

  def isImageFile(path: String): Boolean = {

    val lowerPath =
      path.toLowerCase

    lowerPath.endsWith(".jpg") ||
    lowerPath.endsWith(".jpeg") ||
    lowerPath.endsWith(".png") ||
    lowerPath.endsWith(".gif") ||
    lowerPath.endsWith(".bmp") ||
    lowerPath.endsWith(".webp")
  }
}