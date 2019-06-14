package data_generation

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{PCA, VectorAssembler}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
object readData extends App {

  //Dataset API based ML pipeline
  override def main(args:Array[String]): Unit ={
    //val conf = new SparkConf().setAppName("RDD Read and PCA Application").setMaster("local")
    //val sc = new SparkContext(conf)

    //val dataRDD = sc.textFile("hdfs://localhost:9000/temp/highdim_temp_rdd")

    //dataRDD.take(4).toString.foreach(println)
    //println(dataRDD.count())

    val spark = SparkSession
      .builder()
      .appName("Read File/Apply PCA")
      .master("local[*]")
      .getOrCreate()

    //val conf = new SparkConf().setAppName("Read/Apply").setMaster("local")
    //val sc = new SparkContext(conf)

    val path = "hdfs://localhost:9000/temp/temp_rdd_50000"
    val temp_rdd = spark.sparkContext.textFile(path)

    println("read complete")
    //println(calcRDDSize(temp_dataset))
    temp_rdd.take(4).foreach(println)

    val parsedData = temp_rdd
      .map(s => Row.fromSeq(s.split(',').map(_.toDouble)))

    //parsedData.take(10).toString.foreach(println)

    val columns = path.takeRight(5).toInt
    println("columns" + columns)
    val colNames = (1 to columns).mkString(",")
    val sch = StructType(colNames.split(",").map(fieldname => StructField(fieldname, DoubleType, true)))
    val df = spark.sqlContext.createDataFrame(parsedData,sch)


    //val ds = temp_dataset.map(line => line.split(",").map(arr => arr.map(s => s.toDouble)))

    val assembler = new VectorAssembler()
      .setInputCols(df.schema.fields.map(_.name))
      .setOutputCol("features")

    //Split the data for training set and test set

    val trainingAndTestSet = df.randomSplit(Array(0.7,0.3))
    val training = trainingAndTestSet(0).cache()
    val test = trainingAndTestSet(1)

    val data = assembler.transform(training).select("features")
    applyPCA(data,spark)

  }

  def applyLinearRegression(df: DataFrame,spark: SparkSession): Unit ={

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val lr_model = lr.fit(df)
    println(s"Coefficients: ${lr_model.coefficients} Intercept: ${lr_model.intercept}")

  }

  def applyPCA(df: DataFrame, spark:SparkSession): Unit ={
    //k partition number
    val k = 5

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(k)
      .fit(df)

    val result = pca.transform(df).select("pcaFeatures")
    result.show(false)
  }

  def calcRDDSize(rdd: RDD[String]): Long = {
    rdd.map(_.getBytes("UTF-8").length.toLong)
      .reduce(_+_) //add the sizes together
  }




}
