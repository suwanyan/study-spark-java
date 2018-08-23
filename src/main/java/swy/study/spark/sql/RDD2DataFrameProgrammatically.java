package swy.study.spark.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * 以编程的方式动态指定元数据，将RDD转换为DataFrame                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
 * @author swy
 *
 */
public class RDD2DataFrameProgrammatically {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("RDD2DataFrameProgrammatically")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		JavaRDD<String> lines = sc.textFile(
				"E://swy//resource//workspace-neno//spark-study-java//txt//students.txt");
		
		JavaRDD<Row> stuRow = lines.map(new Function<String, Row>(){
			
			private static final long serialVersionUID = 1L;

			public Row call(String line) throws Exception {
				String[] lineSpited = line.split(",");
				
				return RowFactory.create(Integer.valueOf(lineSpited[0]),
						lineSpited[1],
						Integer.valueOf(lineSpited[2]));
			}
			
		});
		
		//构造元数据
		List<StructField> structFiled = new ArrayList<StructField>();
		structFiled.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
		structFiled.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		structFiled.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		StructType structType = DataTypes.createStructType(structFiled);
		
		//转换为DataFrame
		DataFrame stuDF = sqlContext.createDataFrame(stuRow, structType);
		
		//注册临时表
		stuDF.registerTempTable("students");
		
		//执行sql语句
		DataFrame tenagerDF = sqlContext.sql("select * from students where age<=18");
		
		//FataFrame转换为RDD
		JavaRDD<Row> rowRDD = tenagerDF.javaRDD();
		
		List<Row> row = rowRDD.collect();
		System.out.println(row);
	}
}
