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


    // 2. Détecte les nouveaux fichiers (automatique grâce au checkpoint)


    // 3. Charger le contenu des nouveaux fichiers en binaire

    // 4. Décoder binaire JPG --> pixel numériques 

    // 5. Préparer les données pour le modèle ML

    // 6. Inférence modèle

    // 7. Récupérer les prédictions du modèle 

    // 8. Ecrire les prédictions dans un fichier de sortie

    // 9. Checkpoint sauvegarde l'état (automatique)
}