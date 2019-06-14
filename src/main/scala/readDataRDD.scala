import org.apache.spark.mllib.linalg.{Matrix, Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//Linear Algebra and ML Pipeline with RDD-Based MLlib
object readDataRDD extends App{
  override def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("RDD Read and PCA Application").setMaster("local")
    val sc = new SparkContext(conf)
    val path = "hdfs://localhost:9000/temp/temp_rdd_50000"
    val dataRDD = sc.textFile(path)

    val trainingAndTestSet = dataRDD.randomSplit(Array(0.7,0.3))
    val training = trainingAndTestSet(0)
    val test = trainingAndTestSet(1)

    //Converting RDD[String] to RDD[Vector]

    val parsedData = training
      .map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()

    //Applying Principal Component Analysis
    val mat: RowMatrix = new RowMatrix(parsedData)

    //k - number of principal components
    val k = 100
    // Principal components are stored in a local dense matrix.
    val pc: Matrix = mat.computePrincipalComponents(k)

    // Project the rows to the linear space spanned by the top 4 principal components.
    val projected: RowMatrix = mat.multiply(pc)

    //Applying Linear Regression
    /*val parsedData = createLabeledPoint(training)
    LinearRegression(parsedData)
    */


    //Statistical calculations and correlation matrix calculation
    /*
    val summary: MultivariateStatisticalSummary = Statistics.colStats(training)
    println(summary.mean)  // a dense vector containing the mean value for each column
    println(summary.variance)  // column-wise variance
    println(summary.numNonzeros)  // number of nonzeros in each column

    val correlMatrix: Matrix = Statistics.corr(training, "pearson")
    println(correlMatrix.toString)
    */
  }


  def createLabeledPoint(rdd: RDD[String]): RDD[LabeledPoint] ={

    val parsedData = rdd.map{ line =>
      val x: Array[String] = line.split(",")
      val y = x.map{(a => a.toDouble)}
      val d = y.size -1
      val c = Vectors.dense(y(0),y(d))
      //Labeled Point has a format of (labels (type double), features (Vector))
      LabeledPoint(y(0),c)
    }
    parsedData.cache()

  }


  //Linear Regression
  //Predicts zero for most of the time, makes sense if we keep in mind that the RDD is 90% sparse
  //The training set got a mean sqaured error of 0.03349015181696709 for the first try
  def LinearRegression(rdd:RDD[LabeledPoint]): Unit ={
    // Building the model
    val numIterations = 100
    val stepSize = 0.00000001
    val model = LinearRegressionWithSGD.train(rdd, numIterations, stepSize)

    val valuesAndPreds = rdd.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    valuesAndPreds.foreach((result) => println(s"predicted label: ${result._1}, actual label: ${result._2}"))

    val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2) }.mean()
    println("training Mean Squared Error = " + MSE)

  }



}
