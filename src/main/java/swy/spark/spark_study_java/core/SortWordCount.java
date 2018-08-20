package swy.spark.spark_study_java.core;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class SortWordCount {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("ActionOperation")
				.setMaster("local");
				//.setMaster("spark://192.168.43.124:7077");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("E://swy//resource//workspace-neno//spark-study-java//spark.txt");
		
		// The word count
				JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

					private static final long serialVersionUID = 1L;

					public Iterable<String> call(String t) throws Exception {
						return Arrays.asList(t.split(" "));  
					}
					
				});
				
				JavaPairRDD<String, Integer> pairs = words.mapToPair(
						
						new PairFunction<String, String, Integer>() {

							private static final long serialVersionUID = 1L;

							public Tuple2<String, Integer> call(String t) throws Exception {
								return new Tuple2<String, Integer>(t, 1);
							}
							
						});
				
				JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(
						
						new Function2<Integer, Integer, Integer>() {

							private static final long serialVersionUID = 1L;

							public Integer call(Integer v1, Integer v2) throws Exception {
								return v1 + v2;
							}
							
						});
				
				// Reverse mapping of key-value
				JavaPairRDD<Integer, String> countWords = wordCounts.mapToPair(
						
						new PairFunction<Tuple2<String,Integer>, Integer, String>() {

							private static final long serialVersionUID = 1L;

							public Tuple2<Integer, String> call(Tuple2<String, Integer> t)
									throws Exception {
								return new Tuple2<Integer, String>(t._2, t._1);
							}
							
						});
				
				// sort by key
				JavaPairRDD<Integer, String> sortedCountWords = countWords.sortByKey(false);
				
				// Reverse mapping of key-value
				JavaPairRDD<String, Integer> sortedWordCounts = sortedCountWords.mapToPair(
						
						new PairFunction<Tuple2<Integer,String>, String, Integer>() {

							private static final long serialVersionUID = 1L;

						
							public Tuple2<String, Integer> call(Tuple2<Integer, String> t)
									throws Exception {
								return new Tuple2<String, Integer>(t._2, t._1);
							}
							
						});
				
				// print
				sortedWordCounts.foreach(new VoidFunction<Tuple2<String,Integer>>() {
					
					private static final long serialVersionUID = 1L;

					public void call(Tuple2<String, Integer> t) throws Exception {
						System.out.println(t._1 + " appears " + t._2 + " times.");  	
					}
					
				});
				
				//  close JavaSparkContext
				sc.close();
    }
}
