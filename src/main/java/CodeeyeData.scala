import java.io.FileInputStream

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{PCA, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{Metadata, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.control.Breaks._
import scala.collection.mutable.Map
import scala.io.BufferedSource
object CodeeyeData extends App {

  override def main(args: Array[String]){

    val filename = "/Users/ngungor/Downloads/data-master/rescored-208-210-all.out"
    val conf = new SparkConf().setAppName("Read Code-Eye Data").setMaster("local[*]").setExecutorEnv("spark.debug.maxToStringFields","100")
    val spark = new SparkContext(conf)
    var stream = new FileInputStream(filename)
    val reader = new BufferedSource(stream)
    println(reader.getLines().take(10).foreach(println))

    val parser = new FrameParser(stream)

    val columnMap = mapColumnName(parser)

    val columnList = columnMap.keySet

    /* for debugging purposes
    columnList.foreach(item => if(item == "termstatistics_docfrequency_lexer_types_method_signature_return_type_boolean"){
      println("double")
    })
    */
    val schema = createSchema(columnMap)
    val df = mapColumnValues(parser,columnMap,schema,spark)
    stream.close()
    spark.stop()


  }

  //Creates a map of Column names and index
  def mapColumnName(frameParser: FrameParser): Map[String,Int] ={
    var row_count = 0
    var column_count:Int = 0
    var columnName: Map[String,Int] = Map()

    while(frameParser.hasNext == true && row_count<4){
      val temp_object = frameParser.next()
      //val entrySet = temp_object.entrySet()
      val iterator = temp_object.entrySet().iterator()
        while(iterator.hasNext == true){
          val entry = iterator.next()
          //println(entry.getKey)
          column_count = column_count + 1
          var entry_name = entry.getKey.replace('.','_')
          //entry_name = entry_name + "_"+ row_count
          if(columnName.contains(entry_name) == false) {
            columnName += (entry_name -> (column_count))
          }
        }
      row_count +=1
    }
    println("column count" + column_count)
    println(columnName.size)

    columnName
  }

  //Creates a StructType schema from column names
  def createSchema(columnMap: Map[String, Int]): StructType = {
    val columnName = columnMap.keySet.toArray
    println(columnName.size)
    val array_name = new Array[StructField](columnName.size)
    for(i <- 0 to (columnName.size-1)){
      array_name.update(i,new StructField(columnName.apply(i),StringType,true,Metadata.empty))
    }
    val schema = new StructType(array_name)
    println("schema finished")
    schema
  }

  //Maps the column names to values and converts this map to a DataFrame
  def mapColumnValues(frameParser: FrameParser,columnMap: Map[String,Int], schema : StructType,sparkContext: SparkContext): DataFrame ={
    var row_count = 0
    val columnName = columnMap.keySet
    var rows: Map[String,String] = collection.mutable.Map[String,String]()
    var merged:RDD[Row] = sparkContext.emptyRDD[Row]

    while(frameParser.hasNext == true && row_count<4) {
      columnName.foreach(item => rows.+=(item->"0.0"))
      val temp_object = frameParser.next()
      val iterator = temp_object.entrySet().iterator()
      while (iterator.hasNext == true) {
        val entry = iterator.next()
        //println(entry)
        val entry_key = entry.getKey.replace('.','_')
        if (columnName.contains(entry_key)) {
          var entry_value = entry.getValue.toString
          if(entry_value.contains('"')){
             println("contains extra")
             entry_value = entry_value.substring(1,entry_value.size-1)
             println(entry_value)
          }
          rows.update(entry_key,entry_value)
        }
      }
      row_count += 1

      val (keys, values) = rows.toList.unzip
      val rdd = sparkContext.parallelize(Seq(Row(values:_*)))

      merged = merged.union(rdd)

    }


    val sQLContext = new SQLContext(sparkContext)


    val df= sQLContext.createDataFrame(merged,schema)
    //df.show()
    df
   }

  //cleans the data, converts the values to doubles and finds the optimal principal component value for PCA calculation
  //returns the optimal principal component
  def findPrincipalComponent(dataFrame:DataFrame,columnMap:Map[String,Int],sparkContext: SparkContext): Int ={

    val columnNameList = columnMap.keySet

    //Had to filter out the column names involving Boolean at the end for now as it created an ambiguity issue for filter function
    val droppedColumns:List[String] = List("filename","p4_change","p4_committer", "p4_reviewer", "p4_date",
      "p4_time", "p4_gusid", "p4_filecount","gus_worktype", "fix_filename", "fi_p4_gusid", "fix_gus_worktype",
      "fix_p4_change", "label", "date_release", "date_label", "date_days","fix_p4_gusid","p4_action","p4_version",
      "avgIdentifierLength","blockCount","fieldCount","p4_filecount", "maxBlockNestLevel", "maxMethodLineCount",
      "methodCount", "tokenCount", "variableCount","varsPerMethodCount",
      "termstatistics_docfrequency_lexer_types_method_signature_return_type_boolean",
      "termstatistics_tfidf_lexer_types_field_boolean",
      "termstatistics_termfrequency_lexer_types_method_signature_return_type_Boolean",
      "termstatistics_termfrequency_lexer_types_field_boolean",
      "termstatistics_termfrequency_lexer_types_field_Boolean",
      "termstatistics_termfrequency_lexer_types_method_signature_return_type_boolean",
      "termstatistics_tfidf_lexer_types_method_signature_return_type_boolean",
      "termstatistics_docfrequency_lexer_types_field_boolean",
      "termstatistics_docfrequency_lexer_types_field_Boolean",
      "termstatistics_tfidf_lexer_types_method_signature_return_type_Boolean",
      "termstatistics_docfrequency_lexer_types_method_signature_return_type_Boolean",
      "termstatistics_tfidf_lexer_types_field_Boolean",
      "termstatistics_termfrequency_lexer_types_field_String")

    //Had to remove the column 'termstatistics_termfrequency_lexer_types_field_String' as it had a String value and did not add up to the computation

    dataFrame.show()
    val colsToSelect = dataFrame.columns.filter(item => !droppedColumns.contains(item)).toSeq
   // colsToSelect.mkString(",")


    println("printing column names")
    colsToSelect.foreach(println)

    //val updatedDF = dataFrame.select(dataFrame.columns.filter(colName => colsToSelect.contains(colName)).map(colName => new Column(colName)): _*)
    val updatedDF = dataFrame.select(colsToSelect.head,colsToSelect.tail:_*)

    /* for debugging purposes
    if(updatedDF.columns.contains("p4_filecount")){
      println("contains")
    }else
      println("doesn't contain")

*/
    updatedDF.show()

    //converting to a Row Matrix
    val rowMatrix = updated_convertDouble(updatedDF,sparkContext)

    println("printing RowMatrix")
    rowMatrix.rows.foreach(println)
    println("printing row size")
    rowMatrix.rows.foreach(s => println(s.size))

    //Apply SVD to the matrix
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = rowMatrix.computeSVD(5, computeU = true)
    val U: RowMatrix = svd.U  // The U factor is a RowMatrix.
    val s: Vector = svd.s     // The singular values are stored in a local dense vector.
    val V: Matrix = svd.V     // The V factor is a local dense matrix.

    var principalComponent = 0
    val total_sum = s.toArray.reduceLeft[Double](_+_)
    //
    breakable {
      for (i <- 5 to s.size) {
        //variance = sum of first k elements / sum of all the elements
        val sum_elements= for (k <- 0 to i) {
          s.toArray.take(k).reduceLeft[Double](_+_)
        }
        println("print total sum" + total_sum)
        println("print sum of elements " + sum_elements)
        //val variance =  (sum_elements / total_sum)
        val variance = 0.999
        if ((1 - variance) < 0.01) {
          principalComponent = i
          break
        }
        // 1- variance must be less than or equal to 0.01 for an optimal k value
      }
    }

   principalComponent

  }

  def updated_convertDouble(df: DataFrame,sc:SparkContext): RowMatrix = {
    var final_matrix = Array.ofDim[Double](df.count().toInt, df.columns.size)
    df.rdd.map{ r: Row =>
      for (k <- 0 to df.count().toInt - 1) {
        val dArray = Array.ofDim[Double](r.length)
        for (i <- 0 to r.length - 1) {
          dArray.update(i, r.get(i).toString.toDouble)
        }
        final_matrix.update(k, dArray)
      }
    }
    val rows = sc.parallelize(final_matrix.map(r => Vectors.dense(r)))
    val rowMatrix = new RowMatrix(rows)

    rowMatrix
  }

  def linearRegressionWithVectorFormat(dataFrame: DataFrame, maxCategories: Int) = {

    val vectorAssembler = new VectorAssembler()
        .setInputCols(dataFrame.columns)
        .setOutputCol("features")

    //Add the input and output column definitions
    val vectorIndexer = new VectorIndexer()
      //.setInputCol()
      //.setOutputCol()
      .setMaxCategories(maxCategories)
      .fit(dataFrame)

    val lr = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setRegParam(0.1)
      .setElasticNetParam(1.0)
      .setMaxIter(10)

    val pipeline = new Pipeline().setStages(Array(vectorAssembler, vectorIndexer, lr))

    val Array(training, test) = dataFrame.randomSplit(Array(0.8, 0.2), seed = 12345)

    val model = pipeline.fit(training)

    val fullPredictions = model.transform(test).cache()
    val predictions = fullPredictions.select("prediction").rdd.map(_.getDouble(0))
    val labels = fullPredictions.select("label").rdd.map(_.getDouble(0))
    val RMSE = new RegressionMetrics(predictions.zip(labels)).rootMeanSquaredError
    println(s"  Root mean squared error (RMSE): $RMSE")
  }

  def PCAanalysis(dataFrame: DataFrame, principalComponenet: Int): Unit ={
    val assembler = new VectorAssembler()
      .setInputCols(dataFrame.columns)
      .setOutputCol("features")

    val featuresDF = assembler.transform(dataFrame).select("features")

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(principalComponenet)
      .fit(featuresDF)

    val result = pca.transform(featuresDF).select("pcaFeatures")
    result.show(false)


  }

}
