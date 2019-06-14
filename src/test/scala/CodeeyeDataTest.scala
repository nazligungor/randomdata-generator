
import java.io.ByteArrayInputStream

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{Metadata, StringType, StructField, StructType}
import org.scalatest.FunSuite
import scala.collection.mutable.Map

class CodeeyeDataTest extends FunSuite with SharedSparkContext with DataFrameSuiteBase{

  //Unit tests for three main methods in CodeEye: mapColumnName, createSchema and mapColumnValues
  test("CodeeyeData.createSchema"){
    val temp_columnmap: Map[String,Int] = Map("name" -> 1,"class" -> 2,"year" -> 3)
    val temp_schema = StructType(Array(
      StructField("year",StringType,true,Metadata.empty),
      StructField("name",StringType,true,Metadata.empty),
      StructField("class",StringType,true,Metadata.empty)
    )
    )
    assert(CodeeyeData.createSchema(temp_columnmap) === temp_schema)

  }

  test("CodeeyeData.mapColumnName"){
    val temp_columnmap: Map[String,Int] = Map("name" -> 1,"class" -> 2,"year" -> 3)

    val file_input = "{\"name\":\"nazli\",\"class\":\"java\",\"year\":\"2020\"}"

    val parser = new FrameParser(new ByteArrayInputStream(file_input.getBytes()))

    assert(CodeeyeData.mapColumnName(parser)===temp_columnmap)

  }

  test("CodeeyeData.mapColumnValues"){

    val temp_columnmap: Map[String,Int] = Map("name" -> 1,"class" -> 2,"year" -> 3)

    val file_input = "{\"name\":\"nazli\",\"class\":\"java\",\"year\":\"2020\"}"

    val parser = new FrameParser(new ByteArrayInputStream(file_input.getBytes()))

    val temp_schema = StructType(Array(
      StructField("name",StringType,true,Metadata.empty),
      StructField("class",StringType,true,Metadata.empty),
      StructField("year",StringType,true,Metadata.empty)
    )
    )

    val list = Seq("\"nazli\"","\"java\"","\"2020\"")
    val rows: RDD[Row] = sc.parallelize(List(Row(list: _*)))
    val sQLContext = new SQLContext(sc)
    val temp_df = sQLContext.createDataFrame(rows,temp_schema)

    temp_df.show
    val result_df = CodeeyeData.mapColumnValues(parser,temp_columnmap,temp_schema,sc)
    result_df.show

    assertDataFrameEquals(temp_df,result_df)


  }



}
