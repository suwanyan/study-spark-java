package swy.study.spark.core;

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

public class WordCountCluster {
	public static void main (String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("WordCountCluster")
				.setMaster("spark://192.168.43.124:7077");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile("hdfs://spark1:9000/spark.txt");
		
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Iterable<String> call(String line) throws Exception {
			      	// TODO Auto-generated method stub
				return Arrays.asList(line.split(" "));
			}
		});
		
		JavaPairRDD<String, Integer> pairs = words.mapToPair(
				new PairFunction<String, String, Integer>(){
			private static final long serialVersionUID = 1L;
			
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		
		JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(
				new Function2<Integer, Integer, Integer>(){
					private static final long serialVersionUID = 1L;
					
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				});
		
		wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>(){
			private static final long serialVersionUID = 1L;
			
			public void call(Tuple2<String, Integer> wordCount) throws Exception {
				System.out.println(wordCount._1 + " appears" + wordCount._2 + "times");
			}
		});
		sc.close();
	}
}
