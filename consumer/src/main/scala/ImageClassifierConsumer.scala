// ======================================================================
// CONSUMER FOR PROCESSING REAL TIME DATA
//   mode "train"   : écrit features + labels pour entraîner le modèle
//   mode "predict" : charge le modèle ONNX et écrit les prédictions
// ======================================================================

import java.awt.RenderingHints
import java.awt.image.BufferedImage
import java.io.{ByteArrayInputStream, File, FileWriter, PrintWriter}
import java.time.{Instant, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.Locale
import javax.imageio.ImageIO

import scala.collection.JavaConverters._
import scala.collection.mutable

import ai.onnxruntime.{OnnxTensor, OrtEnvironment, OrtSession}
import com.typesafe.config.ConfigFactory

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, regexp_extract, round, size, spark_partition_id, udf}
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.types._

// Résultat de l'UDF de prétraitement : le vecteur + le temps de décodage
case class Prepared(features: Array[Float], decode_ms: Double)

// Résultat de l'UDF de prédiction : label, probabilité + le temps d'inférence
case class Prediction(predicted_label: String, probability: Float, predict_ms: Double)

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
// Statistiques de performance, écrites dans deux CSV
//   perf.csv        : une ligne "bloc" tous les 100 images
//                     + une ligne "total" quand le flux s'arrête
//                     (moyenne, médiane, min, max en ms de la durée
//                      des batchs et du temps d'attente des images)
//   perf_images.csv : une ligne par image (ses temps et sa prédiction)
// Les deux fichiers sont complétés à la fin : les tests s'accumulent.
// ----------------------------------------------------------------------
object PerfTracker {

  case class Stats(avg: Double, median: Double, min: Double, max: Double)

  // moyenne, médiane, min, max d'une liste de durées (None si la liste est vide)
  def stats(values: Seq[Long]): Option[Stats] =
    if (values.isEmpty) None
    else {
      val s = values.sorted
      val n = s.size
      val median =
        if (n % 2 == 1) s(n / 2).toDouble
        else (s(n / 2 - 1) + s(n / 2)) / 2.0
      Some(Stats(s.sum.toDouble / n, median, s.head.toDouble, s.last.toDouble))
    }

  // nombres au format français (virgule) pour ouvrir les CSV directement dans Excel
  def num(d: Double): String = "%.1f".formatLocal(Locale.FRANCE, d)
  def num2(d: Double): String = "%.2f".formatLocal(Locale.FRANCE, d)

  val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  val msFormatter   = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  val statsHeader: String = Seq(
    "type", "date", "batch_size", "interval_ms", "max_files_per_trigger",
    "image_de", "image_a", "nb_batchs", "duree_s", "debit_img_s",
    "batch_ms_moy", "batch_ms_med", "batch_ms_min", "batch_ms_max",
    "attente_ms_moy", "attente_ms_med", "attente_ms_min", "attente_ms_max"
  ).mkString(";")

  val imagesHeader: String = Seq(
    "batch_size", "interval_ms", "max_files_per_trigger", "batch_id",
    "image", "true_label", "predicted_label", "probability", "correct",
    "arrivee", "debut_batch", "attente_ms", "decodage_ms", "prediction_ms", "partition"
  ).mkString(";")

  // ajoute des lignes à la fin d'un CSV (écrit l'en-tête si le fichier est nouveau)
  def appendLines(path: String, header: String, lines: Seq[String]): Unit = {
    val file  = new File(path)
    val isNew = !file.exists()
    if (file.getParentFile != null) file.getParentFile.mkdirs()
    val out = new PrintWriter(new FileWriter(file, true))
    try {
      if (isNew) out.println(header)
      lines.foreach(out.println)
    } finally out.close()
  }
}

class PerfTracker(statsCsv: String, imagesCsv: String,
                  batchSize: String, intervalMs: String, maxFiles: Int,
                  blockSize: Int = 100) {
  import PerfTracker._

  // temps d'attente des images, rangés par numéro de batch en attendant le bilan du batch
  private val pendingWaits = mutable.Map[Long, Seq[Long]]()

  // bloc de 100 images en cours
  private val blockBatchMs = mutable.ArrayBuffer[Long]()
  private val blockWaitMs  = mutable.ArrayBuffer[Long]()
  private var blockImages  = 0L
  private var blockStartMs = 0L

  // depuis le début du flux
  private val allBatchMs  = mutable.ArrayBuffer[Long]()
  private val allWaitMs   = mutable.ArrayBuffer[Long]()
  private var totalImages = 0L
  private var startMs     = 0L
  private var lastEndMs   = 0L
  private var finished    = false

  // appelé par writeBatch : écrit une ligne par image dans perf_images.csv
  // et garde les temps d'attente pour les statistiques du bloc
  def addImages(batchId: Long, rows: Seq[Row]): Unit = synchronized {
    pendingWaits(batchId) = rows.map(_.getAs[Long]("wait_ms"))

    val lines = rows.map { r =>
      Seq(
        batchSize, intervalMs, maxFiles.toString, batchId.toString,
        r.getAs[String]("image_path").split("/").last,
        r.getAs[String]("true_label"),
        r.getAs[String]("predicted_label"),
        num2(r.getAs[Float]("probability").toDouble),
        r.getAs[Boolean]("correct").toString,
        r.getAs[java.sql.Timestamp]("arrival_time").toLocalDateTime.format(msFormatter),
        r.getAs[java.sql.Timestamp]("prediction_time").toLocalDateTime.format(msFormatter),
        r.getAs[Long]("wait_ms").toString,
        num2(r.getAs[Double]("decode_ms")),
        num2(r.getAs[Double]("predict_ms")),
        r.getAs[Int]("partition").toString
      ).mkString(";")
    }
    appendLines(imagesCsv, imagesHeader, lines)
  }

  // appelé par le listener après chaque batch contenant des images
  def onBatch(batchId: Long, batchStartMs: Long, durationMs: Long, nImages: Long): Unit = synchronized {
    if (finished) reset() // un nouveau flux commence après un arrêt : on repart de zéro

    val waits = pendingWaits.remove(batchId).getOrElse(Seq.empty)

    if (startMs == 0L) startMs = batchStartMs
    if (blockImages == 0L) blockStartMs = batchStartMs
    lastEndMs = batchStartMs + durationMs

    blockBatchMs += durationMs
    allBatchMs   += durationMs
    blockWaitMs ++= waits
    allWaitMs   ++= waits
    blockImages  += nImages
    totalImages  += nImages

    if (blockImages >= blockSize) flushBlock()
  }

  // appelé par le listener quand Spark ne trouve plus d'image (environ 10 s après le dernier batch)
  def onIdle(): Unit = synchronized {
    if (totalImages > 0 && !finished) {
      if (blockImages > 0) flushBlock() // dernier bloc incomplet (moins de 100 images)

      println(s"[Perf] ===== Flux terminé : $totalImages images traitées =====")
      writeStatsRow("total", 1, totalImages, allBatchMs, allWaitMs, startMs, lastEndMs)
      finished = true
    }
  }

  private def flushBlock(): Unit = {
    writeStatsRow("bloc", totalImages - blockImages + 1, totalImages, blockBatchMs, blockWaitMs, blockStartMs, lastEndMs)
    blockBatchMs.clear()
    blockWaitMs.clear()
    blockImages = 0L
  }

  private def reset(): Unit = {
    pendingWaits.clear()
    blockBatchMs.clear(); blockWaitMs.clear(); blockImages = 0L; blockStartMs = 0L
    allBatchMs.clear();   allWaitMs.clear();   totalImages = 0L; startMs = 0L; lastEndMs = 0L
    finished = false
  }

  // écrit une ligne de statistiques dans perf.csv et l'affiche en console
  private def writeStatsRow(kind: String, from: Long, to: Long,
                            batchMs: Seq[Long], waitMs: Seq[Long],
                            fromMs: Long, toMs: Long): Unit = {

    val images  = to - from + 1
    val seconds = (toMs - fromMs) / 1000.0
    val rate    = if (seconds > 0) images / seconds else 0.0
    val b       = stats(batchMs)
    val w       = stats(waitMs)

    def cells(s: Option[Stats]): Seq[String] =
      s.map(x => Seq(num(x.avg), num(x.median), num(x.min), num(x.max))).getOrElse(Seq("", "", "", ""))

    val line = (Seq(
      kind, LocalDateTime.now().format(dateFormatter), batchSize, intervalMs, maxFiles.toString,
      from.toString, to.toString, batchMs.size.toString, num(seconds), num(rate)
    ) ++ cells(b) ++ cells(w)).mkString(";")

    appendLines(statsCsv, statsHeader, Seq(line))

    // affichage console
    val label = if (kind == "total") "TOTAL" else s"images $from-$to"
    println(s"[Perf] $label : ${num(seconds)} s (${num(rate)} img/s) | ${batchMs.size} batchs")
    b.foreach(x => println(s"[Perf]   batch   (ms) : moy ${num(x.avg)} | méd ${num(x.median)} | min ${num(x.min)} | max ${num(x.max)}"))
    w.foreach(x => println(s"[Perf]   attente (ms) : moy ${num(x.avg)} | méd ${num(x.median)} | min ${num(x.min)} | max ${num(x.max)}"))
  }
}

object ImageClassifierConsumer {

  // Taille d'entrée du modèle : toutes les images sont ramenées à 64x64
  val ImgSize = 64

  // Log horodaté (utilisé seulement au démarrage)
  val timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  def log(message: String): Unit =
    println(s"[${LocalDateTime.now().format(timestampFormatter)}] $message")

  // Durée écoulée depuis startNanos, en millisecondes
  def elapsedMs(startNanos: Long): Double = (System.nanoTime() - startNanos) / 1e6

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

  // Prétraitement chronométré : renvoie le vecteur + la durée du décodage
  def timedPreprocess(bytes: Array[Byte]): Prepared = {
    val start    = System.nanoTime()
    val features = preprocessImage(bytes)
    Prepared(features, elapsedMs(start))
  }

  // --------------------------------------------------------------------
  // Features --> prédiction du modèle ONNX (chronométrée)
  //   entrée du modèle  : "input"  = matrice [1 image x 12 288 features]
  //   sorties du modèle : label prédit + probabilités de chaque classe
  // --------------------------------------------------------------------
  def predict(features: Seq[Float], modelPath: String): Prediction = {
    val session = OnnxModel.get(modelPath)
    val start   = System.nanoTime() // après le chargement : on ne mesure que l'inférence
    val tensor  = OnnxTensor.createTensor(OrtEnvironment.getEnvironment, Array(features.toArray))

    try {
      val result = session.run(Map("input" -> tensor).asJava)
      try {
        val labels = result.get(0).getValue.asInstanceOf[Array[String]]
        val probas = result.get(1).getValue.asInstanceOf[Array[Array[Float]]]
        Prediction(labels(0), probas(0).max, elapsedMs(start))
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
    val perfCsv            = config.getString("consumer.perf.csv")         // statistiques tous les 100 images
    val perfImagesCsv      = config.getString("consumer.perf.images.csv")  // une ligne par image

    // On relit la config du producer pour noter batch.size et interval.ms dans les CSV
    val producerConfFile = new File(config.getString("consumer.perf.producer.conf"))
    val producerConf     = ConfigFactory.parseFile(producerConfFile)
    val prodBatchSize    =
      if (producerConf.hasPath("producer.batch.size")) producerConf.getInt("producer.batch.size").toString else "?"
    val prodIntervalMs   =
      if (producerConf.hasPath("producer.interval.ms")) producerConf.getLong("producer.interval.ms").toString else "?"

    val perf = new PerfTracker(perfCsv, perfImagesCsv, prodBatchSize, prodIntervalMs, maxFilesPerTrigger)

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
    // asNondeterministic : on extrait 2 champs du résultat (features, decode_ms),
    // Spark ne doit pas appeler l'UDF une fois par champ
    val preprocessUDF = udf((bytes: Array[Byte]) => timedPreprocess(bytes))
      .asNondeterministic()

    val preparedData = imageStream
      .withColumn("prep", preprocessUDF(col("content")))
      .select(
        col("path").as("image_path"),
        // label = ce qui est avant le "_" dans le nom : dest/lys_000016.jpg -> lys
        regexp_extract(col("path"), "([^/]+)_[^/_]+$", 1).as("label"),
        // heure à laquelle le producer a déposé le fichier dans dest/
        col("modificationTime").as("arrival_time"),
        col("prep.features").as("features"),
        col("prep.decode_ms").as("decode_ms"),
        // numéro de la partition (donc de la tâche parallèle) qui a traité l'image
        spark_partition_id().as("partition")
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
          .withColumn("prediction_time", current_timestamp()) // début du micro-batch
          .select(
            col("image_path"),
            col("label").as("true_label"),
            col("prediction.predicted_label"),
            col("prediction.probability"),
            col("arrival_time"),
            col("prediction_time"),
            // temps passé par l'image dans dest/ avant que son micro-batch démarre
            ((col("prediction_time").cast("double") - col("arrival_time").cast("double")) * 1000)
              .cast("long").as("wait_ms"),
            round(col("decode_ms"), 2).as("decode_ms"),
            round(col("prediction.predict_ms"), 2).as("predict_ms"),
            col("partition")
          )
          .withColumn("correct", col("true_label") === col("predicted_label"))
      }

    // ----------------------------------------------------------
    // 8. Écriture : pour chaque micro-batch, on ajoute les lignes
    //    en Parquet, dans perf_images.csv, et on affiche le tableau
    // ----------------------------------------------------------
    def writeBatch(batchDF: DataFrame, batchId: Long): Unit = {
      batchDF.persist() // évite de recalculer les UDF (écriture + affichage)

      batchDF.write
        .mode("append")
        .parquet(outputDir)

      if (mode == "train") {
        println(s"Batch $batchId : ${batchDF.count()} image(s) écrite(s) dans $outputDir")
      } else {
        // une ligne par image dans perf_images.csv + temps d'attente pour les statistiques
        // (au plus maxFilesPerTrigger lignes : on peut les ramener sur le driver)
        perf.addImages(batchId, batchDF.collect().toSeq)

        println(s"--- Batch $batchId ---")
        batchDF
          .select(
            regexp_extract(col("image_path"), "[^/]+$", 0).as("image"),
            // temps mis par l'image pour être récupérée (attente dans dest/, en ms)
            col("wait_ms").as("attente_ms"),
            col("true_label"),
            col("predicted_label"),
            col("probability"),
            col("correct")
          )
          .show(maxFilesPerTrigger, truncate = false)
      }

      batchDF.unpersist()
    }

    // ----------------------------------------------------------
    // Mesure des performances : appelé par Spark après chaque micro-batch
    // ----------------------------------------------------------
    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(e: QueryStartedEvent): Unit = {}
      override def onQueryTerminated(e: QueryTerminatedEvent): Unit = {}

      override def onQueryProgress(e: QueryProgressEvent): Unit = {
        val p = e.progress
        if (p.numInputRows > 0)
          perf.onBatch(p.batchId, Instant.parse(p.timestamp).toEpochMilli, p.batchDuration, p.numInputRows)
        else
          perf.onIdle() // plus aucune image : bilan final
      }
    })

    val query = result.writeStream
      .foreachBatch(writeBatch _)
      .option("checkpointLocation", checkpointDir)
      .start()

    log(s"Consumer lancé - mode $mode")
    println(s"Master: $master (${spark.sparkContext.defaultParallelism} tâches en parallèle)")
    println(s"Source: $inputDir | $maxFilesPerTrigger image(s) max par batch")
    println(s"Producer: batch.size = $prodBatchSize | interval.ms = $prodIntervalMs")
    println(s"Sortie: $outputDir")
    println(s"Statistiques: $perfCsv (tous les 100) | $perfImagesCsv (par image)")
    println(s"Checkpoint: $checkpointDir")

    query.awaitTermination()
  }
}