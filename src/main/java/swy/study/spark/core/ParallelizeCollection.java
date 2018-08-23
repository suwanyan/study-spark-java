package swy.study.spark.core;


import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class ParallelizeCollection {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("ParallelizeCollection")
				.setMaster("local");
		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8 ,9, 10);
		JavaRDD<Integer> numRDD = sc.parallelize(nums);
		int sum = numRDD.reduce(new Function2<Integer, Integer, Integer>(){
			private static final long serialVersionUID = 1L;
			 public Integer call(Integer num1, Integer num2) throws Exception {
				 return num1 + num2;
			 }
		});
		System.out.println(sum);
		sc.close();
	}
}
