package data_generation

import org.apache.spark.ml.feature.{PCA, VectorAssembler}
import org.apache.spark.mllib.linalg.{DenseMatrix, DenseVector, SparseMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, concat, concat_ws, lit}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuilder => MArrayBuilder}
import scala.util.Random



object createDummyDataset extends App {

  override def main(args: Array[String]): Unit = {
    //gets the column and row information for the dummy dataset

    println("Enter # of rows")
    val rows = scala.io.StdIn.readInt()

    println("Enter # of columns ")
    val columns = scala.io.StdIn.readInt()

    println("Enter # of rows for sparse matrix")
    val rows_sparse = scala.io.StdIn.readInt()

    println("Enter # of columns for sparse matrix")
    val columns_sparse = scala.io.StdIn.readInt()

    //For PCA analysis
    println("Enter k value")
    val k = scala.io.StdIn.readInt()

    val cName = Array.ofDim[String](columns)
    for (a <- 0 to columns - 1) {
      cName(a) = randomString(5)
      //println(cName(a))
    }

    val t1 = System.nanoTime()

//    val spark = SparkSession
//      .builder()
//      .appName("CreateDummyDataset")
//      .master("local[*]")
//      .getOrCreate()

    val conf = new SparkConf().setAppName("Dataset Generator").setMaster("local")
    val sc = new SparkContext(conf)
    //val ds = createDataset(rows, columns, cName, spark)
    //applyPCA(ds,k)
    //def rand = scala.util.Random.nextInt(100) +1
    //val rdd = new data_generation.RandomRDD(sc,10,rows,( rand, rand, rand, rand))
    //val sqlContext = new SQLContext(sc)
    //import sqlContext.implicits._
    //val df = rdd.toDF()
    //df.show()

    val n = rows*columns
    val randDouble:Array[Double] = (1 to n map(_ => (Random.nextDouble()+1))).toArray
    //randDouble.foreach(println)
    val temp = createDenseM(columns,rows,randDouble)
    val temp_sparse = toSparseMatrix(temp,columns_sparse,rows_sparse,randDouble)
    println("number of elements " + temp_sparse.numCols * temp_sparse.numRows)
    println("number of nonzero elements " + temp_sparse.numNonzeros)
    println(temp_sparse.toString(20,20))
    val temp_rdd = toRDD(temp_sparse,sc)
    println("temp rdd created")

    //val result_rdd = applyPCA(sc, temp_rdd,k)
    val duration = (System.nanoTime - t1) / 1e9d
    //println("number of rows: " + df.count())
    println("Duration: " + duration)

    


  }

  def createDataset(rows: Int, columns: Int, colN: Array[String], spark: SparkSession): DataFrame= {

    val data = (1 to rows).map(_ => Seq.fill(columns)(1 + Random.nextDouble()))
    val colNames = (1 to columns).mkString(",")
    val sch = StructType(colNames.split(",").map(fieldname => StructField(fieldname, DoubleType, true)))

    val rdd = spark.sparkContext.parallelize(data.map(x => Row(x: _*)))

    val rdd_size = calcRDDSize(rdd.map(_.toString()))
    val ds = spark.sqlContext.createDataFrame(rdd,sch)

    ds.write.format("com.databricks.spark.csv").mode("overwrite").save("Users/ngungor/IdeaProjects/scala_projectone/temp_file.csv")
    //ds.printSchema()

    println("File created")
    println("Size: " + rdd_size)
    //ds.show()

    //val scaledDatad =ds
    //  .rdd
    // .map{
    //    row => Vectors.dense(row.getAs[Seq[Double]]("").toArray)
    //  }

    //val mat: RowMatrix = new RowMatrix(scaledDatad)

    ds

  }
  def applyPCA(sc:SparkContext, ds: DataFrame,k:Int): DataFrame ={
    val assembler = new VectorAssembler()
      .setInputCols(ds.schema.fields.map(_.name))
      .setOutputCol("features")

    val df = assembler.transform(ds)

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(k)
      .fit(ds)

    val result = pca.transform(df).select("pcaFeatures")
    result.show(false)

    //val disassembler = new org.apache.spark.ml.feature.VectorDisassembler()

    val temp_df = result.toDF()

    val rddofDF = temp_df.rdd.map(_.toString())

    val pca_result_size = calcRDDSize(rddofDF)

    println("After pca: " + pca_result_size)
    //rddofDF.coalesce(1)
    //  .write.format("com.databricks.spark.csv")
    //  .option("header","true")
    //  .save("Users/ngungor/IdeaProjects/scala_projectone/temp_file_pca.csv")


    //stringifyArrays(temp_df).write.csv("Users/ngungor/IdeaProjects/scala_projectone/temp_file_pca.csv")
    // df_final.write.format("com.databricks.spark.csv").mode("overwrite").save("Users/ngungor/IdeaProjects/scala_projectone/temp_file_pca.csv")
    //println("error here?")

    //println(mat.toString())

    //val svd: SingularValueDecomposition[RowMatrix, Matrix]= mat.computeSVD(5, computeU = true)
    //val U: RowMatrix = svd.U
    //val s: Vector = svd.s
    //val V: Matrix = svd.V

    //println(U.toString(5,Int.MaxValue))
    temp_df
  }
  def calcRDDSize(rdd: RDD[String]): Long = {
    rdd.map(_.getBytes("UTF-8").length.toLong)
      .reduce(_+_) //add the sizes together
  }

  def stringifyArrays(dataFrame: DataFrame): DataFrame = {
    val colsToStringify = dataFrame.schema.filter(p => p.dataType.typeName == "array").map(p => p.name)

    colsToStringify.foldLeft(dataFrame)((df, c) => {
      df.withColumn(c, concat(lit("["), concat_ws(", ", col(c).cast("array<string>")), lit("]")))
    })
  }

  def randomString(length: Int) = {
    val r = new scala.util.Random
    val sb = new StringBuilder
    for (i <- 1 to length) {
      sb.append(r.nextPrintableChar)
    }
    sb.toString
  }

  def createDenseM(colNum: Int, rowNum: Int, values: Array[Double]): DenseMatrix ={
    val temp_matrix = new DenseMatrix(rowNum,colNum,values)
    temp_matrix
  }

  def toSparseMatrix(dm : DenseMatrix, colNum:Int,rowNum:Int,vals:Array[Double]): SparseMatrix ={
    val spVals: MArrayBuilder[Double] = new MArrayBuilder.ofDouble
    val colPtrs: Array[Int] = new Array[Int](colNum + 1)
    val rowIndices: MArrayBuilder[Int] = new MArrayBuilder.ofInt
    val col_dm = dm.numCols
    val row_dm = dm.numRows
    var n = 0
    var j = 1
    while(j < colNum ){
      var i = 1
      var index = i + (j-1)*colNum
      while(i < rowNum ){
         var v = 0.0
        if(index < col_dm*row_dm) {
          print("index "+index)
          v = vals.apply(index)
          println("value " + v)
        }
        if (v != 0.0) {
            rowIndices += i
            spVals += v
            n += 1
          }
        i += 1
        index += 1
      }
      j += 1
      colPtrs(j) = n
    }
    new SparseMatrix(rowNum, colNum, colPtrs, rowIndices.result(), spVals.result())
  }

  def toRDD(m : SparseMatrix, s:SparkContext):RDD[DenseVector]={
    val columns = m.toArray.grouped(m.numRows)
    val rows = columns.toSeq.transpose
    val vectors = rows.map(row => new DenseVector(row.toArray))
    val rdd_out = s.parallelize(vectors)

    rdd_out
  }

}

