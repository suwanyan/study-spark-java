package swy.study.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 使用json文件创建DataFrame
 * @author swy
 *
 */
public class DataFrameCreate {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("DataFrameCreate")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame df = sqlContext.read().json(
				"E://swy//resource//workspace-neno//spark-study-java//txt//student.json");
		df.show();
	}
}
