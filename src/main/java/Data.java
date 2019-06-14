import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.omg.CORBA.Any;
import scala.Tuple2;

import java.io.FileInputStream;
import java.util.*;

public class Data {

    public static void main(String[] args){

        //the big data file
        //edit with the full directory
        String filename = "/Users/ngungor/Downloads/data-master/rescored-208-210-all.out";


        SparkConf conf = new SparkConf()
                .setAppName("Code-eye SparkApp")
                .setMaster("local[*]")
                .set("spark.executor.memory", "6g");

        //create Spark context
        JavaSparkContext spark = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(spark);

        StructField[] structFields = new StructField[]{
                new StructField("termstatistics.documentlength", DataTypes.IntegerType, true,Metadata.empty()),
                new StructField("filename",DataTypes.StringType,true,Metadata.empty()),
                new StructField("p4.action",DataTypes.StringType,true,Metadata.empty()),
                new StructField("p4.change",DataTypes.StringType,true,Metadata.empty()),
                new StructField("p4.committer",DataTypes.StringType,true,Metadata.empty()),
                new StructField("p4.reviewer",DataTypes.StringType,true,Metadata.empty()),
                new StructField("p4.date",DataTypes.StringType,true,Metadata.empty()),
                new StructField("p4.time",DataTypes.StringType,true,Metadata.empty()),
                new StructField("p4.gusid",DataTypes.StringType,true,Metadata.empty()),
                new StructField("p4.filecount",DataTypes.StringType,true,Metadata.empty()),
                new StructField("gus.worktype",DataTypes.StringType,true,Metadata.empty()),
                new StructField("fix.filename",DataTypes.StringType,true,Metadata.empty()),
                new StructField("fix.p4.gusid",DataTypes.StringType,true,Metadata.empty()),
                new StructField("fix.gus.worktype",DataTypes.StringType,true,Metadata.empty()),
                new StructField("fix.p4.change",DataTypes.StringType,true,Metadata.empty()),
                new StructField("p4.version",DataTypes.StringType,true,Metadata.empty()),
                new StructField("label",DataTypes.StringType,true,Metadata.empty()),
                new StructField("date.release",DataTypes.StringType,true,Metadata.empty()),
                new StructField("date.label",DataTypes.StringType,true,Metadata.empty()),
                new StructField("date.days",DataTypes.StringType,true,Metadata.empty()),
                new StructField("avgIdentifierLength",DataTypes.StringType,true,Metadata.empty()),
                new StructField("blockCount",DataTypes.StringType,true,Metadata.empty()),
                new StructField("fieldCount",DataTypes.StringType,true,Metadata.empty()),
                new StructField( "termstatistics.termfrequency.lexer.types.field.boolean", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.docfrequency.lexer.types.field.boolean", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.tfidf.lexer.types.field.boolean", DataTypes.LongType,true,Metadata.empty()),
                new StructField( "termstatistics.termfrequency.lexer.types.local_variable.EntityCommon", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.docfrequency.lexer.types.local_variable.EntityCommon", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.tfidf.lexer.types.field.local_variable.EntityCommon", DataTypes.LongType,true,Metadata.empty()),
                new StructField( "termstatistics.termfrequency.lexer.types.local_variable.ManageableStateEnum", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.docfrequency.lexer.types.local_variable.ManageableStateEnum", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.tfidf.lexer.types.field.local_variable.ManageableStateEnum", DataTypes.LongType,true,Metadata.empty()),
                new StructField( "termstatistics.termfrequency.lexer.types.local_variable.boolean", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.docfrequency.lexer.types.local_variable.boolean", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.tfidf.lexer.types.field.local_variable.boolean", DataTypes.LongType,true,Metadata.empty()),
                new StructField( "termstatistics.termfrequency.lexer.types.method_signature.parameter.List", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.docfrequency.lexer.types.method_signature.parameter.List", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.tfidf.lexer.types.field.method_signature.parameter.List", DataTypes.LongType,true,Metadata.empty()),
                new StructField( "termstatistics.termfrequency.lexer.types.method_signature.parameter.ManageableStateEnum", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.docfrequency.lexer.types.method_signature.parameter.ManageableStateEnum", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.tfidf.lexer.types.field.method_signature.parameter.ManageableStateEnum", DataTypes.LongType,true,Metadata.empty()),
                new StructField( "termstatistics.termfrequency.lexer.types.method_signature.parameter.Map", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.docfrequency.lexer.types.method_signature.parameter.Map", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.tfidf.lexer.types.field.method_signature.parameter.Map", DataTypes.LongType,true,Metadata.empty()),
                new StructField( "termstatistics.termfrequency.lexer.types.method_signature.parameter.Map<String,ExtendedPackageInfo>", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.docfrequency.lexer.types.method_signature.parameter.Map<String,ExtendedPackageInfo>", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.tfidf.lexer.types.field.method_signature.parameter.Map<String,ExtendedPackageInfo>", DataTypes.LongType,true,Metadata.empty()),
                new StructField( "termstatistics.termfrequency.lexer.types.method_signature.parameter.Map<String,ManageableStateEnum>", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.docfrequency.lexer.types.method_signature.parameter.Map<String,ManageableStateEnum>", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.tfidf.lexer.types.field.method_signature.parameter.Map<String,ManageableStateEnum>", DataTypes.LongType,true,Metadata.empty()),
                new StructField( "termstatistics.termfrequency.lexer.types.method_signature.parameter.Map<String,VersionInfo>", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.docfrequency.lexer.types.method_signature.parameter.Map<String,VersionInfo>", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.tfidf.lexer.types.field.method_signature.parameter.Map<String,VersionInfo>", DataTypes.LongType,true,Metadata.empty()),
                new StructField( "termstatistics.termfrequency.lexer.types.method_signature.parameter.PackageMemberNode", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.docfrequency.lexer.types.method_signature.parameter.PackageMemberNode", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.tfidf.lexer.types.field.method_signature.parameter.PackageMemberNode", DataTypes.LongType,true,Metadata.empty()),
                new StructField( "termstatistics.termfrequency.lexer.types.method_signature.parameter.String", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.docfrequency.lexer.types.method_signature.parameter.String", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.tfidf.lexer.types.field.method_signature.parameter.String", DataTypes.LongType,true,Metadata.empty()),
                new StructField( "termstatistics.termfrequency.lexer.types.method_signature.parameter.boolean", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.docfrequency.lexer.types.method_signature.parameter.boolean", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.tfidf.lexer.types.field.method_signature.parameter.boolean", DataTypes.LongType,true,Metadata.empty()),
                new StructField( "termstatistics.termfrequency.lexer.types.method_signature.return_type.ImageElement", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.docfrequency.lexer.types.method_signature.return_type.ImageElement", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.tfidf.lexer.types.field.method_signature.return_type.ImageElement", DataTypes.LongType,true,Metadata.empty()),
                new StructField( "termstatistics.termfrequency.lexer.types.method_signature.return_type.boolean", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.docfrequency.lexer.types.method_signature.return_type.boolean", DataTypes.IntegerType,true,Metadata.empty()),
                new StructField( "termstatistics.tfidf.lexer.types.field.method_signature.return_type.boolean", DataTypes.LongType,true,Metadata.empty()),
                new StructField("maxBlockNestLevel",DataTypes.StringType,true,Metadata.empty()),
                new StructField("maxMethodLineCount",DataTypes.StringType,true,Metadata.empty()),
                new StructField("methodCount",DataTypes.StringType,true,Metadata.empty()),
                new StructField("tokenCount",DataTypes.StringType,true,Metadata.empty()),
                new StructField("variableCount",DataTypes.StringType,true,Metadata.empty()),
                new StructField("varsPerMethodCount",DataTypes.StringType,true,Metadata.empty())
        };
        StructType structType = DataTypes.createStructType(structFields);


        try{
            FileInputStream fin = new FileInputStream(filename);
            int i=fin.read();
            System.out.print((char)i);
            FrameParser parser = new FrameParser(fin);
            List<JsonObject> temp = new ArrayList<JsonObject>();

            //Map to harvest all the column names
            Map<String,Integer> columnName = new HashMap<String, Integer>();
            int count = 0;
            int row_counter = 0;
            Map<String,String> rows = new HashMap<String,String>();


            while(parser.hasNext() == true && row_counter<100){
                JsonObject temp_object = parser.next();
                for (Map.Entry<String, JsonElement> e : temp_object.entrySet()) {
                    columnName.putIfAbsent(e.getKey(),count++);
                }

                row_counter++;
            }

            List<String> list = new ArrayList<String>(columnName.keySet());

            List<List<String>> list_rows = new ArrayList<>();

            JavaRDD<String> vals = spark.parallelize(list);
            row_counter = 0;

            System.out.println(vals.take(5));


            while(parser.hasNext() && row_counter<100) {
                JsonObject temp_object = parser.next();
                for(String s : columnName.keySet()){
                    rows.put(s,null);
                }
                for (Map.Entry<String, JsonElement> e : temp_object.entrySet()) {
                    if(rows.containsKey(e.getKey())){
                        rows.put(e.getKey(),e.getValue().toString());
                    }
                }

                list_rows.add(new ArrayList<>(rows.values()));
                JavaRDD<String> row_rdd = spark.parallelize(new ArrayList<>(rows.values()));
                spark.union(vals,row_rdd);

                row_counter++;
            }

            System.out.println(vals.count());

            System.out.println(columnName.keySet().size());

            System.out.println(vals.take(10));

            Dataset df = createDataFrame(vals,sqlContext);

            df.printSchema();
            System.out.println(df.take(5));

            //System.out.println(temp.get(1));
            //System.out.println(temp.get(4).toString());
            //System.out.println(temp.size());
            
            //List<JsonObject> testData = temp.subList(0,100);

            //System.out.println(testData.get(0));
            //System.out.println(testData.get(20));

            //Seq<String> seq = JavaConverters.asScalaIteratorConverter(testData.iterator()).asScala().toSeq();

            //Dataset df = sqlContext.read().schema(structType).json(vals);
            //JavaRDD items = sqlContext.read().schema(structType).json(vals).toJavaRDD();
            //items.filter(item -> item != null);
            //items.foreach(item -> System.out.println(item));
            //System.out.println(vals.take(10));
            //Dataset df = sqlContext.read().json(vals);
            //df.printSchema();
            //df.show(10);

            PairFunction<JsonObject, String, Any> tk2V2PairFunction = new PairFunction<JsonObject, String, Any>() {
                @Override
                public Tuple2<String,Any> call(JsonObject jsonObject) throws Exception {
                    String temp = jsonObject.toString();
                    //Tuple2<String,Any> tuple = new Tuple2<String,Any>();
                    return null;
                }
            };
            //JavaPairRDD<String,Any> pairRDD = vals.mapToPair(tk2V2PairFunction);

            fin.close();
            //spark.close();
        }catch(Exception e){System.out.println(e);}
    }


    static Dataset<Row> createDataFrame(JavaRDD<String> lines, SQLContext sq) {
        JavaRDD<Row> rows = lines.map(new Function<String, Row>(){
            private static final long serialVersionUID = -4332903997027358601L;

            @Override
            public Row call(String line) throws Exception {
                return RowFactory.create(Arrays.asList(line.split("\\s+")));
            }
        });
        StructType schema = new StructType(new StructField[] {
                new StructField("words",
                        DataTypes.createArrayType(DataTypes.StringType), false,
                        Metadata.empty()) });

        Dataset<Row> wordDF = sq.createDataFrame(rows, schema);

        return wordDF;
    }

    }


