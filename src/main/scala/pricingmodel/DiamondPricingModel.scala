package pricingmodel 

import org.apache.spark.sql.SparkSession

object DiamondPricingModel extends App{

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[1]")
    .appName("PricingModel")
    .getOrCreate()

  // load dataset to spark from a file
  val path = "diamonds.csv"
  val df = spark.read
    .format("com.databricks.spark.csv")
    .option("header","true")
    .option("inferSchema", "true")
    .load(path)

  import spark.implicits._

  // transformation I: color to rank using Spark StringIndexer
  import org.apache.spark.ml.feature.StringIndexer
  val indexerColor = new StringIndexer()
    .setInputCol("color")
    .setOutputCol("color_rank")
    .setStringOrderType("alphabetDesc")
    .fit(df)
  val df_with_color_rank = indexerColor.transform(df)


  // transformation II: cut to rank (from worse to best)
  val cutMap: Map[String, Int] = Map(
    "Fair" -> 1,
    "Good" -> 2,
    "Very Good" -> 3,
    "Premium" -> 4,
    "Ideal" -> 5
  )

  // transformation III: clarity to rank (from worse to best)
  val clarityMap: Map[String, Int] = Map(
    "I1"   -> 1,
    "SI2"  -> 2,
    "SI1"  -> 3,
    "VS2"  -> 4,
    "VS1"  -> 5,
    "VVS2" -> 6,
    "VVS1" -> 7,
    "IF"   -> 8
  )

  // final pre-processed dataset
  val df_transformed = df_with_color_rank.map(row => {
    val price = row.getAs[Int]("price")
    val carat = row.getAs[Double]("carat")
    val depth = row.getAs[Double]("depth")
    val table = row.getAs[Double]("table")
    val cut = row.getAs[String]("cut")
    val clarity = row.getAs[String]("clarity")
    val x = row.getAs[Double]("x")
    val y = row.getAs[Double]("y")
    val z = row.getAs[Double]("z")
    val color_rank = row.getAs[Double]("color_rank")
    
    (price, carat, depth, table, cutMap(cut), clarityMap(clarity), color_rank, x, y, z)
  }).toDF("price", "carat", "depth", "table", "cut_rank", "clarity_rank", "color_rank", "x", "y", "z")

  // separate label from features columns
  val label = "price"
  val features = Array("carat", "depth", "table", "cut_rank", "clarity_rank", "color_rank", "x", "y", "z")


  // assemble of features
  import org.apache.spark.ml.feature.VectorAssembler

  val assembler = new VectorAssembler()
    .setInputCols(features)
    .setOutputCol("features")
  val df_assembled = assembler.transform(df_transformed)

  // training using Spark ML pipeline (grid search and Random Forest)
  import org.apache.spark.ml.regression.RandomForestRegressor
  import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
  import org.apache.spark.ml.evaluation.RegressionEvaluator
  import org.apache.spark.ml.Pipeline

  val df_split = df_assembled.randomSplit(Array(0.7, 0.3))
  val df_training = df_split(0)
  val df_testing = df_split(1)

  val model = new RandomForestRegressor()
    .setLabelCol("price")
    .setFeaturesCol("features")

  val paramGrid = new ParamGridBuilder()
    .addGrid(model.maxDepth, Array(5, 8))
    //.addGrid(rfModel.numTrees, [20, 60])
    .build()

  val stages = Array(model)
  val pipeline = new Pipeline().setStages(stages)

  val cv = new CrossValidator()
    .setEstimator(pipeline)
    .setEstimatorParamMaps(paramGrid)
    .setEvaluator(new RegressionEvaluator().setLabelCol("price"))

  val pipelineFitted = cv.fit(df_training)

  // evaluation on testing dataset
  import org.apache.spark.mllib.evaluation.RegressionMetrics

  val holdout = pipelineFitted.bestModel
    .transform(df_testing)
    .selectExpr(
      "double(round(prediction)) as prediction", 
      "double(price) as price" 
    )

  val rm = new RegressionMetrics(holdout.rdd.map(row => (row(0), row(1))))
  println(s"MAE: ${rm.meanAbsoluteError}")
  println(s"RMSE Squared: ${rm.rootMeanSquaredError}")
  println(s"R Squared: ${rm.r2}")
  println(s"Explained Variance ${rm.explainedVariance}")
}
