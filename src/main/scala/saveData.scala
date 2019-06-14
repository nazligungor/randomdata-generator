import org.apache.spark.mllib.linalg.{DenseMatrix, DenseVector, SparseMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuilder => MArrayBuilder}
import scala.reflect.ClassTag

object saveData extends App{
  override def main(args: Array[String]): Unit ={

    println("Enter # of rows")
    val rows = scala.io.StdIn.readInt()

    println("Enter # of columns")
    val columns = scala.io.StdIn.readInt()

    println("Enter # of partitions")
    val partitions = scala.io.StdIn.readInt()

    //println("Enter # of columns for sparse matrix")
    //val columns_sparse = scala.io.StdIn.readInt()

    //create random densematrix
    val conf = new SparkConf().setAppName("Dataset Generator").setMaster("local")
    val sc = new SparkContext(conf)
    var c = BigInt(columns)
    var r = BigInt(rows)
    val n = c * r
    println(n)
    println(n.toInt)
    //val randDouble:Array[Double] = (1 to n.toInt map(_ => (Random.nextDouble()))).toArray
    //val randDouble = new Array[Double](n.toInt)
    //for (i <- 0 until randDouble.length) { randDouble(i) = 1 }
    //randDouble.foreach(println)
    //println(randDouble.take(5).toString)

    val totalPartition = rows / 1000
    val denseRows = rows*(0.1)
    val densePartition = (denseRows/1000).toInt
    val sparsePartition = (totalPartition - densePartition)


    val temp_rdd = sparseRDD(densePartition,sparsePartition,columns,sc)

    val path = "hdfs://localhost:9000/temp/temp_rdd_" + columns

    println(path)

    temp_rdd.map(x => x.mkString(",")).saveAsTextFile(path)

    sc.stop()


  }
  def createDenseM(colNum: Int, rowNum: Int, values: Array[Double]): DenseMatrix ={
    val temp_matrix = new DenseMatrix(rowNum,colNum,values)
    temp_matrix
  }

  def toSparseMatrix(dm : DenseMatrix, colNum:Int,rowNum:Int,vals:Array[Double]): SparseMatrix ={
    val spVals: MArrayBuilder[Double] = new MArrayBuilder.ofDouble
    val colPtrs: Array[Int] = new Array[Int](colNum + 1)
    val rowIndices: MArrayBuilder[Int] = new MArrayBuilder.ofInt
    val col_dm = BigInt(dm.numCols)
    val row_dm = BigInt(dm.numRows)
    var n = 0
    var j = 1
    while(j < colNum ){
      var i = 1
      var index = i + (j-1)*colNum
      while(i < rowNum ){
        var v = 0.0
        if(index < (col_dm*row_dm).toInt) {
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


  //This is a new method so that the matrix with the given size will not be created at once and the matrix will not be
  //created in the driver node. Instead initially a smaller size of RDD is created and using repartition RDD size is
  //increased in a distributed way

  //The fixed rows per partition = 1000
  def sparseRDD(densePartition: Int, sparsePartition: Int, columnCount: Int, sc: SparkContext): RDD[Array[Double]] ={
    val rowsPerPartition = 1000
    val totalPartition = densePartition + sparsePartition
    val temp_array_1 = Array.ofDim[Double](densePartition,columnCount)

    val temp_rdd_1 = sc.parallelize(temp_array_1)

    val repartition_dense = temp_rdd_1.repartition(densePartition)

    val rdd_1 = repartition_dense.flatMap(
      i => Array.fill(rowsPerPartition,columnCount)(Math.random)
    )

    val temp_array_2 = Array.ofDim[Double](sparsePartition,columnCount)
    val temp_rdd_2  = sc.parallelize(temp_array_2)
    val repartition_sparse = temp_rdd_2.repartition(sparsePartition)

    val rdd_2 = repartition_sparse.flatMap(
      i => Array.fill(rowsPerPartition,columnCount)(0.0)
    )

    val merged = rdd_1 ++ rdd_2

    merged
  }

  def cast[A](a: Any, tt: ClassTag[A]): A = a.asInstanceOf[A]
}
