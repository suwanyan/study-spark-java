package swy.study.spark.core;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 取文本中最大的前3个数字
 * @author swy
 *
 */
public class TopN {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("ActionOperation")
				.setMaster("local");
				//.setMaster("spark://192.168.43.124:7077");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile("E://swy//resource//workspace-neno//spark-study-java//txt//topN.txt");
		
		JavaPairRDD<Integer, String> pairs = lines.mapToPair(new PairFunction<String, Integer, String>(){
			
			private static final long serialVersionUID = 1L;
			
			public Tuple2<Integer, String> call(String s) throws Exception {
				return new Tuple2<Integer, String>(Integer.valueOf(s), s);
			}
			
		});
		
		JavaPairRDD<Integer, String> sortPairs = pairs.sortByKey(false);
		
		JavaRDD<String> sorts = sortPairs.map(new Function<Tuple2<Integer, String>, String>(){
			
			private static final long serialVersionUID = 1L;
			
			public String call(Tuple2<Integer, String> t) throws Exception {
				return t._2;
			}
			
		});
		
		List<String> top3 = sorts.take(3);
		 
		for(String i : top3) {
			System.out.println(i);
		}
		
		sc.close();
	}
}
