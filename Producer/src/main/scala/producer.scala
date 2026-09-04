import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

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

    val deleteSource =
      config.getBoolean("producer.delete.source")

    val overwrite =
      config.getBoolean("producer.overwrite")

    // ----------------------------------------------------------
    // Spark
    // ----------------------------------------------------------

    val spark =
      SparkSession.builder()
        .appName("Image Producer")
        .master("local[*]")
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val sc =
      spark.sparkContext

    log("Producer demarre")

    // ----------------------------------------------------------
    // Decouverte des fichiers
    // ----------------------------------------------------------

    val filesRDD =
      sc.binaryFiles(sourceDir)
        .keys
        .filter(isImageFile)

    val files =
      filesRDD.collect()

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

    // ----------------------------------------------------------
    // Traitement des batches
    // ----------------------------------------------------------

    batches.zipWithIndex.foreach { case (batch, batchIndex) =>

      log(
        s"Debut batch ${batchIndex + 1}/${batches.length} - ${batch.size} image(s)"
      )

      // RDD du batch
      val filesRDD =
        sc.parallelize(batch)

      // --------------------------------------------------------
      // Copie en parallele
      // --------------------------------------------------------

    filesRDD.foreachPartition { partition =>

        val conf =
          new Configuration()

      val sourceFS =
        FileSystem.get(conf)

      val destinationFS =
        FileSystem.get(conf)

      var count = 0

      partition.foreach { file =>

        val sourcePath =
          new Path(file)

        val fileName =
          sourcePath.getName

        val destinationPath =
            new Path(
              s"$destinationDir/$fileName"
            )

          // ----------------------------------------------------
          // Copie d'un fichier
          // ----------------------------------------------------

          try {

            log(s"Copie de $fileName...")

            val startTime =
              System.nanoTime()

        val copied =
          FileUtil.copy(
            sourceFS,
            sourcePath,
            destinationFS,
            destinationPath,
            deleteSource,
            overwrite,
            conf
          )

            val durationMs =
              (System.nanoTime() - startTime) / 1000000

        if (copied) {

              log(
                s"SUCCES : $fileName | ${durationMs} ms"
              )

        } else {

              log(
                s"ECHEC : $fileName | ${durationMs} ms"
              )
            }

          } catch {

            case e: Exception =>

              log(
                s"ERREUR : $fileName | ${e.getClass.getSimpleName} | ${e.getMessage}"
              )
        }
        }

        sourceFS.close()
        destinationFS.close()
      }

      log(
        s"Fin batch ${batchIndex + 1}/${batches.length}"
      )

      // --------------------------------------------------------
      // Pause entre les batches
      // --------------------------------------------------------

      if (batchIndex < batches.length - 1) {

        log(
          s"Attente de ${intervalMs} ms"
          )

          Thread.sleep(intervalMs)
        }
    }

    // ----------------------------------------------------------
    // Fin
    // ----------------------------------------------------------

    log("Toutes les images ont ete traitees")

    spark.stop()

    log("Producer termine")
  }

  // Verification des fichiers images
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