import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.{SparkConf, SparkContext}

object ImageProducer {

  def main(args: Array[String]): Unit = {

    // Configuration
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


    // Spark
    val sparkConf = new SparkConf()
      .setAppName("Image Producer")
      .setIfMissing("spark.master", "local[*]")

    val sc = new SparkContext(sparkConf)

    sc.setLogLevel("WARN")


    // Découverte des fichiers
    val filesRDD =
      sc.binaryFiles(sourceDir)
        .keys
        .filter(isImageFile)


    // Récupération des fichiers
    val files =
      filesRDD.collect()

    println(s"${files.length} fichier(s) trouvé(s).")


    // Distribution des fichiers
    val filesParallelized =
      sc.parallelize(files.toSeq)


    // Copie en parallèle
    filesParallelized.foreachPartition { partition =>

      val conf = new Configuration()

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
          new Path(s"$destinationDir/$fileName")


        // Copie avec Hadoop FileUtil
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


        if (copied) {
          println(s"[Producer] OK : $fileName")
        } else {
          println(s"[Producer] ERREUR : $fileName")
        }


        count += 1


        // Gestion du batch
        if (count % batchSize == 0) {

          println(
            s"Batch de $batchSize fichiers terminé."
          )

          Thread.sleep(intervalMs)
        }
      }

      sourceFS.close()
      destinationFS.close()
    }


    // Arrêt de Spark
    sc.stop()
  }


  // Vérification des fichiers images
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