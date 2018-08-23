package swy.study.spark.core;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class SecondarySort {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("ActionOperation")
				.setMaster("local");
				//.setMaster("spark://192.168.43.124:7077");
		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("E://swy//resource//workspace-neno//spark-study-java//num.txt");
		
		JavaPairRDD<SecondarySortKey, String> pairs = lines.mapToPair(
				new PairFunction<String, SecondarySortKey, String>(){

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public Tuple2<SecondarySortKey, String> call(String line) throws Exception {
						String[] tmp = line.split(" ");
						SecondarySortKey key = new SecondarySortKey(
								Integer.valueOf(tmp[0]),
								Integer.valueOf(tmp[1]));
						return new Tuple2<SecondarySortKey, String>(key, line);
					}
					
				});
		
		JavaPairRDD<SecondarySortKey, String> sortPairs = pairs.sortByKey();
		 
		JavaRDD<String> sortLines = sortPairs.map(
				new Function<Tuple2<SecondarySortKey, String>, String>(){

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public String call(Tuple2<SecondarySortKey, String> t) throws Exception {
						// TODO Auto-generated method stub
						return t._2;
					}
			
		});
		
		sortLines.foreach(new VoidFunction<String>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public void call(String t) throws Exception {
				System.out.println(t);
				
			}
			
		});
		
		sc.close();
	}
}
