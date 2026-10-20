// ======================================================================
// CONSUMER FOR PROCESSING REAL TIME DATA
//   mode "train"   : écrit features + labels pour entraîner le modèle
//   mode "predict" : charge le modèle ONNX et écrit les prédictions
// ======================================================================

import java.awt.RenderingHints
import java.awt.image.BufferedImage
import java.io.{ByteArrayInputStream, File, FileWriter, PrintWriter}
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Locale
import javax.imageio.ImageIO

import scala.collection.JavaConverters._
import scala.collection.mutable

import ai.onnxruntime.{OnnxTensor, OrtEnvironment, OrtSession}
import com.typesafe.config.ConfigFactory

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, regexp_extract, size, udf}
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.types._

// Résultat renvoyé par l'UDF de prédiction (devient une colonne struct dans Spark)
case class Prediction(predicted_label: String, probability: Float)

// ----------------------------------------------------------------------
// Chargement du modèle ONNX une seule fois (et pas à chaque image)
// ----------------------------------------------------------------------
object OnnxModel {
  private var session: OrtSession = _

  def get(modelPath: String): OrtSession = synchronized {
    if (session == null) {
      println(s"Chargement du modèle ONNX : $modelPath")
      session = OrtEnvironment.getEnvironment.createSession(modelPath, new OrtSession.SessionOptions())
    }
    session
  }
}

// ----------------------------------------------------------------------
// Suivi du temps de traitement
//   perf.csv : une ligne par image
//              -> image ; heure_production ; heure_ecriture ;
//                 temps_recuperation_ms (production -> récupération par Spark) ;
//                 temps_ecriture_ms     (récupération -> écriture du résultat)
//              (complété à la fin : les tests s'accumulent)
//   console  : médianes de ces temps par bloc d'images
//              taille du bloc réglable : 1 = chaque image, 100 = tous les 100 images
// ----------------------------------------------------------------------
object BlockTracker {

  val fullFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  // millisecondes -> heure lisible
  def fmt(ms: Long, f: DateTimeFormatter): String =
    Instant.ofEpochMilli(ms).atZone(ZoneId.systemDefault()).toLocalDateTime.format(f)

  def median(values: Seq[Long]): Double = {
    val s = values.sorted
    val n = s.size
    if (n % 2 == 1) s(n / 2).toDouble else (s(n / 2 - 1) + s(n / 2)) / 2.0
  }

  val header: String = "image;heure_production;heure_ecriture;temps_recuperation_ms;temps_ecriture_ms"
}

class BlockTracker(csvPath: String, blockSize: Int) {
  import BlockTracker._

  // (temps de récupération, temps d'écriture) des images en attente d'un bloc complet, en ms
  private val pending    = mutable.ArrayBuffer[(Long, Long)]()
  private var imagesDone = 0L
  private var blockNo    = 0L

  // appelé après chaque batch avec (image, heure de production, heure de récupération, heure d'écriture) en ms
  def add(images: Seq[(String, Long, Long, Long)]): Unit = synchronized {

    // 1. une ligne par image dans perf.csv (en-tête écrit seulement si le fichier est nouveau)
    val file  = new File(csvPath)
    val isNew = !file.exists()
    if (file.getParentFile != null) file.getParentFile.mkdirs()
    val out = new PrintWriter(new FileWriter(file, true))
    try {
      if (isNew) out.println(header)
      images.foreach { case (image, prod, recup, ecr) =>
        out.println(s"$image;${fmt(prod, fullFormatter)};${fmt(ecr, fullFormatter)};${recup - prod};${ecr - recup}")
      }
    } finally out.close()

    // 2. médiane par bloc, affichée quand un bloc est complet
    pending ++= images.map { case (_, prod, recup, ecr) => (recup - prod, ecr - recup) }
    while (pending.size >= blockSize) {
      printBlock(pending.take(blockSize).toList)
      pending.remove(0, blockSize)
    }
  }

  // appelé quand il n'y a plus d'image : affiche le dernier bloc incomplet
  def flushRemaining(): Unit = synchronized {
    if (pending.nonEmpty) {
      printBlock(pending.toList)
      pending.clear()
    }
  }

  private def printBlock(durations: Seq[(Long, Long)]): Unit = {
    blockNo += 1
    val from = imagesDone + 1
    imagesDone += durations.size
    def num(d: Double): String = "%.1f".formatLocal(Locale.FRANCE, d)
    val recup = median(durations.map(_._1))
    val ecr   = median(durations.map(_._2))
    val total = median(durations.map { case (r, e) => r + e })
    println(s"[Bloc $blockNo] images $from-$imagesDone : médianes récupération ${num(recup)} ms" +
            s" | écriture ${num(ecr)} ms | total ${num(total)} ms")
  }
}

object ImageClassifierConsumer {

  // Taille d'entrée du modèle : toutes les images sont ramenées à 64x64
  val ImgSize = 64

  // --------------------------------------------------------------------
  // Octets bruts (JPG) --> vecteur de features pour le modèle
  //   1. décodage JPG avec ImageIO
  //   2. redimensionnement à 64x64 (taille fixe + évite l'OutOfMemory)
  //   3. extraction RGB + normalisation 0-255 -> 0-1
  //   4. aplatissement en un vecteur de 64*64*3 = 12 288 valeurs
  // Renvoie un tableau vide si l'image est illisible.
  // --------------------------------------------------------------------
  def preprocessImage(bytes: Array[Byte]): Array[Float] = {

    val original =
      try {
        ImageIO.read(new ByteArrayInputStream(bytes))
      } catch {
        case e: Exception =>
          println(s"Erreur lors du décodage : ${e.getMessage}")
          null
      }

    // ImageIO.read() renvoie null (sans exception) si le format n'est pas reconnu
    if (original == null) return Array.emptyFloatArray

    // Redimensionnement : on "redessine" l'image originale dans une image 64x64
    val resized = new BufferedImage(ImgSize, ImgSize, BufferedImage.TYPE_INT_RGB)
    val g = resized.createGraphics()
    g.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR)
    g.drawImage(original, 0, 0, ImgSize, ImgSize, null)
    g.dispose()

    // Vecteur aplati : [R, G, B] du pixel (0,0), puis (0,1), ... ligne par ligne
    val features = new Array[Float](ImgSize * ImgSize * 3)
    var i = 0
    for (y <- 0 until ImgSize; x <- 0 until ImgSize) {
      val rgb = resized.getRGB(x, y)
      features(i)     = ((rgb >> 16) & 0xFF) / 255f // Rouge
      features(i + 1) = ((rgb >> 8) & 0xFF) / 255f  // Vert
      features(i + 2) = (rgb & 0xFF) / 255f         // Bleu
      i += 3
    }

    features
  }

  // --------------------------------------------------------------------
  // Features --> prédiction du modèle ONNX
  //   entrée du modèle  : "input"  = matrice [1 image x 12 288 features]
  //   sorties du modèle : label prédit + probabilités de chaque classe
  // --------------------------------------------------------------------
  def predict(features: Seq[Float], modelPath: String): Prediction = {
    val session = OnnxModel.get(modelPath)
    val tensor  = OnnxTensor.createTensor(OrtEnvironment.getEnvironment, Array(features.toArray))

    try {
      val result = session.run(Map("input" -> tensor).asJava)
      try {
        val labels = result.get(0).getValue.asInstanceOf[Array[String]]
        val probas = result.get(1).getValue.asInstanceOf[Array[Array[Float]]]
        Prediction(labels(0), probas(0).max) // probabilité de la classe prédite
      } finally result.close()
    } finally tensor.close()
  }

  def main(args: Array[String]): Unit = {

    // ----------------------------------------------------------
    // Configuration (consumer/src/main/resources/application.conf)
    // ----------------------------------------------------------
    val config = ConfigFactory.load()

    val master             = config.getString("consumer.master")           // ex. "local[*]" ou "local[4]"
    val mode               = config.getString("consumer.mode")             // "train" ou "predict"
    val inputDir           = config.getString("consumer.input.dir")        // = producer.destination.dir
    val maxFilesPerTrigger = config.getInt("consumer.max.files.per.trigger")
    val outputDir          = config.getString(s"consumer.$mode.output.dir")
    val checkpointDir      = config.getString(s"consumer.$mode.checkpoint.dir")
    val perfCsv            = config.getString("consumer.perf.csv")
    val blockSize          = config.getInt("consumer.perf.bloc")           // 1 ou 100 images par bloc

    val tracker = new BlockTracker(perfCsv, blockSize)

    val spark = SparkSession.builder()
      .appName("ImageClassifierConsumer")
      .master(master)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // 1-3. Surveillance du dossier + lecture des nouveaux fichiers en binaire
    // En streaming, Spark exige un schéma connu à l'avance
    val binarySchema = StructType(Seq(
      StructField("path", StringType, nullable = false),
      StructField("modificationTime", TimestampType, nullable = false),
      StructField("length", LongType, nullable = false),
      StructField("content", BinaryType, nullable = true)
    ))

    new java.io.File(inputDir).mkdirs() // crée dest s'il n'existe pas encore

    val imageStream = spark.readStream
      .format("binaryFile")
      .schema(binarySchema)
      .option("maxFilesPerTrigger", maxFilesPerTrigger)
      .option("pathGlobFilter", "*.{jpg,jpeg,JPG,JPEG}")
      .load(inputDir)

    // 4-5. Décodage + préparation pour le modèle via UDF
    val preprocessUDF = udf((bytes: Array[Byte]) => preprocessImage(bytes))

    val preparedData = imageStream
      .select(
        col("path").as("image_path"),
        // label = ce qui est avant le "_" dans le nom : dest/lys_000016.jpg -> lys
        regexp_extract(col("path"), "([^/]+)_[^/_]+$", 1).as("label"),
        // heure de production = heure à laquelle le producer a déposé le fichier dans dest/
        col("modificationTime").as("production_time"),
        preprocessUDF(col("content")).as("features")
      )
      .filter(size(col("features")) > 0) // on écarte les images illisibles

    // ----------------------------------------------------------
    // Ce qu'on écrit selon le mode
    // ----------------------------------------------------------
    val result: DataFrame =
      if (mode == "train") {
        // Phase 1 : features + label pour le service ML (format inchangé pour train.py)
        preparedData.select(col("image_path"), col("label"), col("features"))

      } else {
        // Phase 2 (6-7) : inférence avec le modèle ONNX
        val modelPath = config.getString("consumer.predict.model.path")

        // asNondeterministic : empêche Spark d'appeler l'UDF une fois par champ extrait
        val predictUDF = udf((features: Seq[Float]) => predict(features, modelPath))
          .asNondeterministic()

        preparedData
          .withColumn("prediction", predictUDF(col("features")))
          .select(
            col("image_path"),
            col("label").as("true_label"),
            col("prediction.predicted_label"),
            col("prediction.probability"),
            col("production_time"),
            // heure de récupération = début du micro-batch qui a pris l'image
            current_timestamp().as("recup_time")
          )
          .withColumn("correct", col("true_label") === col("predicted_label"))
      }

    // ----------------------------------------------------------
    // 8. Écriture : pour chaque micro-batch, on ajoute les lignes en Parquet,
    //    on note l'heure d'écriture et on affiche le tableau
    // ----------------------------------------------------------
    def writeBatch(batchDF: DataFrame, batchId: Long): Unit = {
      batchDF.persist() // évite de recalculer les UDF (écriture + affichage)

      batchDF.write
        .mode("append")
        .parquet(outputDir)

      // heure d'écriture = moment où le batch vient d'être écrit dans output/
      val writeMs = System.currentTimeMillis()

      if (mode == "train") {
        println(s"Batch $batchId : ${batchDF.count()} image(s) écrite(s) dans $outputDir")
      } else {
        // tableau des prédictions
        println(s"--- Batch $batchId ---")
        batchDF
          .select(
            regexp_extract(col("image_path"), "[^/]+$", 0).as("image"),
            col("true_label"),
            col("predicted_label"),
            col("probability"),
            col("correct")
          )
          .show(maxFilesPerTrigger, truncate = false)

        // (image, heure de production, heure de récupération, heure d'écriture), dans l'ordre de production
        val times = batchDF
          .select(regexp_extract(col("image_path"), "[^/]+$", 0), col("production_time"), col("recup_time"))
          .collect()
          .map(r => (r.getString(0), r.getTimestamp(1).getTime, r.getTimestamp(2).getTime, writeMs))
          .sortBy(_._2)
          .toSeq
        tracker.add(times)
      }

      batchDF.unpersist()
    }

    // ----------------------------------------------------------
    // Quand Spark ne trouve plus d'image (environ 10 s après la dernière),
    // on écrit le dernier bloc, même s'il est incomplet
    // ----------------------------------------------------------
    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(e: QueryStartedEvent): Unit = {}
      override def onQueryTerminated(e: QueryTerminatedEvent): Unit = {}
      override def onQueryProgress(e: QueryProgressEvent): Unit =
        if (e.progress.numInputRows == 0) tracker.flushRemaining()
    })

    val query = result.writeStream
      .foreachBatch(writeBatch _)
      .option("checkpointLocation", checkpointDir)
      .start()

    println(s"Consumer lancé - mode $mode")
    println(s"Source: $inputDir | $maxFilesPerTrigger image(s) max par batch")
    println(s"Sortie: $outputDir")
    println(s"Temps par image : $perfCsv | médiane par bloc de $blockSize image(s)")
    println(s"Checkpoint: $checkpointDir")

    query.awaitTermination()
  }
}