// ======================================================================
// CONSUMER FOR PROCESSING REAL TIME DATA 
// ======================================================================

// Imports 
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_timestamp}

object ImageClassifierConsumer {

  def main(args: Array[String]): Unit = {

    // Créer une spark session
    val spark = SparkSession.builder()
    .appName("ImageClassifierConsumer")
    .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Définir le chemin du dossier à surveiller
    val inputDir = "./Producer/src/image-destination"

    // Définir le chemin checkpoint pour le suivi de l'état du streaming
    val checkpointDir = "checkpoint/"

    // Définir maxFilesPerTrigger pour contrôler le nombre de fichiers traités par trigger
    val maxFilesPerTrigger = 1

    // 1. Spark readStream surveille le dossier (+ checkpoint compare)
    // 2. Détecte les nouveaux fichiers (automatique grâce au checkpoint)
    // 3. Charger le contenu des nouveaux fichiers en binaire

    // Config readstream : 
        // - format : binaryFile (lire les octets bruts du JPG)
        // - options: maxFilesPerTrigger, recursiveFileLookup
        // source destination : dossier à surveiller
    val imageStream = spark.readStream
      .format("binaryFile") // lire octet bruts du JPG
      .option("maxFilesPerTrigger", maxFilesPerTrigger)
      .option("recursiveFileLookup", "true")
      .load(inputDir)

    // Résultats: DataFrame contenant:
        // path (string)
        // modificationTime (timestamp)
        // length (long)
        // content (binary)

    // Sélectionner les colonnes nécessaires pour le traitement (path, modificationTime, content)
    val selectedData = imageStream
        .select(
            col("path").as("image_path"), 
            col("modificationTime").as("timestamp"), 
            col("content").as("bytes_JPG")
        )


    // 4. Décoder binaire JPG --> pixel numériques 
    def decodeJPG(jpgBytes: Array[Byte]): Array[Array[Array[Int]]] = {
    
        try{
            val inputStream = new ByteArrayInputStream(jpgBytes)

            // ImageIO.rea() décode le JPG en BufferedImage
            val bufferedImage = ImageIO.read(inputStream)

            // récupérer les dimensions de l'image
            val width = bufferedImage.getWidth 
            val height = bufferedImage.getHeight

            // Créer la matrice 3D pour stocker les pixels (hauteur x largeur x canaux)
            val pixels = Array.ofDim[Int](height, width, 3) // 3 canaux pour RGB

            // Boucler sur chaque pixel 
            for (y <- 0 until height; x <- 0 until width) {
                // getRGB() retourne un Int contenant ARGB
                // Bits: [Alpha, Red, Green, Blue]
                val rgb = bufferedImage.getRGB(x, y)

                // Extraire les composantes RGB avec des opérations binaires
                pixels(y)(x)(0) = (rgb >> 16) & 0xFF // Rouge
                pixels(y)(x)(1) = (rgb >> 8) & 0xFF  // Vert
                pixels(y)(x)(2) = rgb & 0xFF         // Bleu
            }

            pixels // retourner la matrice de pixels

        } catch {
            case e: Exception =>
                println(s"Erreur lors du décodage du JPG: ${e.getMessage}")
                Array.ofDim[Int](0, 0, 0) // retourner une matrice vide en cas d'erreur
        }
    }

    // Enregistrer la fonction comme UDF pour l'utiliser dans les transformations Spark
    val decodeJPGUDF = udf((bytes: Array[Byte]) => decodeJPG(bytes))

    val decodedData = selectedData
        .select(
            col("image_path"), 
            col("timestamp"), 
            decodeJPGUDF(col("bytes_JPG")).as("pixels")
        )

    // 5. Préparer les données pour le modèle ML

    // 6. Inférence modèle

    // 7. Récupérer les prédictions du modèle 

    // 8. Ecrire les prédictions dans un fichier de sortie

    // 9. Checkpoint sauvegarde l'état (automatique)

    // Write stream (test)

    val query = decodedData
        .writeStream
        .format("console") // pour le test, écrire dans la console
        .mode("append") // mode append pour ajouter les nouvelles lignes
        .option("checkpointLocation", checkpointDir) // définir le chemin du checkpoint
        .option("numRows", 10) // nombre de lignes à afficher dans la console
        .start()

    println("✅ Consumer lancé - Décodage JPG actif")
    println(s"📁 Source: $inputDir")
    println(s"📁 Checkpoint: $checkpointDir")

    query.awaitTermination()
}
