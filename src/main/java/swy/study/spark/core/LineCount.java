package swy.study.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class LineCount {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("numWordOfFile")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile("E://swy//resource//workspace-neno//spark-study-java//spark.txt");
		JavaPairRDD<String, Integer> pairs = lines.mapToPair(
				new PairFunction<String, String, Integer>(){

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String line) throws Exception {
				return new Tuple2<String, Integer>(line, 1);
			}
			
		});
		
		JavaPairRDD<String, Integer> linesConuts = pairs.reduceByKey(
				new Function2<Integer, Integer, Integer>(){

					private static final long serialVersionUID = 1L;

					public Integer call(Integer v1, Integer v2) throws Exception {
						// TODO Auto-generated method stub
						return v1 + v2;
					}
			
		});
		
		linesConuts.foreach(
				new VoidFunction<Tuple2<String, Integer>>(){

					public void call(Tuple2<String, Integer> t) throws Exception {
						System.out.println(t._1 + " appers " + t._2 + "times");
						
					}
			
		});
		
		sc.close();
	}
}
