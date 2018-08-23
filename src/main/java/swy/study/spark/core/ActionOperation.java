package swy.study.spark.core;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class ActionOperation {
	public static void main(String[] args) {
		//
		SparkConf conf = new SparkConf()
				.setAppName("ActionOperation")
				.setMaster("local");
				//.setMaster("spark://192.168.43.124:7077");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
//----------------------------------初始化RDD----------------------------------------------
		// 
		List<Integer> num = Arrays.asList(1, 2, 3, 4, 5, 6);
		JavaRDD<Integer> numRDD = sc.parallelize(num);
		
//-----------------------------------count()---------------------------------------------
		//
		long count = numRDD.count();
		System.out.println("集合元素个数： " + count);
		
//-----------------------------------collect()---------------------------------------------
		// ,数据量比较大的话不建议使用，性能差，RDD太大容易发生内存溢出
		List<Integer> num1 = numRDD.collect();
		for (Integer i : num1) {
			System.out.println("元素： " + i);
		}
		
//-----------------------------------take(n)---------------------------------------------
		//取前n个元素 ： take(n),和collect类似，只是获取的是前n个
		List<Integer> num2 = numRDD.take(3);
		for (Integer i : num2) {
			System.out.println("前3个元素： " + i);
		}
		
//------------------------------------ reduce--------------------------------------------	
		//
		int sum = numRDD.reduce(new Function2<Integer, Integer, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Integer call(Integer t1, Integer t2) throws Exception {
				// 
				return t1 + t2;
			}
		});
		System.out.println("和： " + sum);
		
//------------------------------------ saveAsTextFile-------------------------------------------			
		//将RDD保存到hdfs
		//numRDD.saveAsTextFile("hdfs://spark1:9000/swy/num.txt");
		
//--------------------------------------countByKey------------------------------------------
		//统计每个key对应value
		@SuppressWarnings("unchecked")
		List<Tuple2<String, String>> score = Arrays.asList(
				new Tuple2<String, String>("c2","a"),
				new Tuple2<String, String>("c1", "b"),
				new Tuple2<String, String>("c1", "c"),
				new Tuple2<String, String>("c1", "d"));
		JavaPairRDD<String, String> cRDD = sc.parallelizePairs(score);
		Map<String, Object> cNumRDD = cRDD.countByKey();
		System.out.println("countByKey");
		for (Map.Entry<String, Object>  m : cNumRDD.entrySet()) {
			System.out.println(m.getKey() + ":" +  m.getValue());
		}
//--------------------------------------------------------------------------------		
		sc.close();
	}


	

}
