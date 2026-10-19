// ======================================================================
// CONSUMER FOR PROCESSING REAL TIME DATA
//   mode "train"   : écrit features + labels pour entraîner le modèle
//   mode "predict" : charge le modèle ONNX et écrit les prédictions
// ======================================================================

import java.awt.RenderingHints
import java.awt.image.BufferedImage
import java.io.ByteArrayInputStream
import javax.imageio.ImageIO

import scala.collection.JavaConverters._

import ai.onnxruntime.{OnnxTensor, OrtEnvironment, OrtSession}
import com.typesafe.config.ConfigFactory

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, regexp_extract, size, udf}
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

    val spark = SparkSession.builder()
      .appName("ImageClassifierConsumer")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ----------------------------------------------------------
    // Configuration (consumer/src/main/resources/application.conf)
    // ----------------------------------------------------------
    val config = ConfigFactory.load()

    val mode               = config.getString("consumer.mode")             // "train" ou "predict"
    val inputDir           = config.getString("consumer.input.dir")        // = producer.destination.dir
    val maxFilesPerTrigger = config.getInt("consumer.max.files.per.trigger")
    val outputDir          = config.getString(s"consumer.$mode.output.dir")
    val checkpointDir      = config.getString(s"consumer.$mode.checkpoint.dir")

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
        preprocessUDF(col("content")).as("features")
      )
      .filter(size(col("features")) > 0) // on écarte les images illisibles

    // ----------------------------------------------------------
    // Ce qu'on écrit selon le mode
    // ----------------------------------------------------------
    val result: DataFrame =
      if (mode == "train") {
        // Phase 1 : features + label pour le service ML
        preparedData.select(col("image_path"), col("label"), col("features"))

      } else {
        // Phase 2 (6-7) : inférence avec le modèle ONNX
        val modelPath = config.getString("consumer.predict.model.path")

        // asNondeterministic : empêche Spark d'appeler l'UDF 2 fois
        // (une fois par champ du struct qu'on extrait juste après)
        val predictUDF = udf((features: Seq[Float]) => predict(features, modelPath))
          .asNondeterministic()

        preparedData
          .withColumn("prediction", predictUDF(col("features")))
          .select(
            col("image_path"),
            col("label").as("true_label"),
            col("prediction.predicted_label"),
            col("prediction.probability"),
            current_timestamp().as("prediction_time")
          )
          .withColumn("correct", col("true_label") === col("predicted_label"))
      }

    // ----------------------------------------------------------
    // 8. Écriture : pour chaque micro-batch, on ajoute les lignes
    //    en Parquet et on affiche un suivi en console
    // ----------------------------------------------------------
    def writeBatch(batchDF: DataFrame, batchId: Long): Unit = {
      batchDF.persist() // évite de recalculer les UDF (écriture + affichage)

      batchDF.write
        .mode("append")
        .parquet(outputDir)

      if (mode == "train") {
        println(s"Batch $batchId : ${batchDF.count()} image(s) écrite(s) dans $outputDir")
      } else {
        println(s"--- Batch $batchId ---")
        batchDF
          .select(
            regexp_extract(col("image_path"), "[^/]+$", 0).as("image"),
            col("true_label"),
            col("predicted_label"),
            col("probability"),
            col("correct")
          )
          .show(truncate = false)
      }

      batchDF.unpersist()
    }

    val query = result.writeStream
      .foreachBatch(writeBatch _)
      .option("checkpointLocation", checkpointDir)
      .start()

    println(s"Consumer lancé - mode $mode")
    println(s"Source: $inputDir")
    println(s"Sortie: $outputDir")
    println(s"Checkpoint: $checkpointDir")

    query.awaitTermination()
  }
}