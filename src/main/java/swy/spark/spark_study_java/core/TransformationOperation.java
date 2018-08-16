package swy.spark.spark_study_java.core;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;


public class TransformationOperation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("WordCountLocal")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		printNum (sc);
		printStr(sc);
		groupBykey(sc);
		sortByKey(sc);
		joinOrCogroup(sc);
		sc.close();

	}
	public static void printNum (JavaSparkContext sc) {

		List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5, 6);
		JavaRDD<Integer> numRDD = sc.parallelize(nums);
		//JavaRDD<Integer> mulNum = map(numRDD);
		JavaRDD<Integer> mulNum = filter(numRDD);
		
		mulNum.foreach(new VoidFunction<Integer>() {
			
			/**
			 * print
			 */
			private static final long serialVersionUID = 1L;

			public void call(Integer num) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(num);
				
			}
		});
	}
	private static JavaRDD<Integer> map(JavaRDD<Integer> numRDD) {
		/**
		 * map
		 * Multiply each element in the set by 2
		 */
		JavaRDD<Integer> mulNum = numRDD.map(
				new Function<Integer, Integer>(){
					private static final long serialVersionUID = 1L;

					public Integer call(Integer num) throws Exception {
						return num * 2;
					}
			
		});
		return mulNum;
	}
	
	private static JavaRDD<Integer> filter (JavaRDD<Integer> numRDD) {
		/**
		 * filter
		 * Filters even Numbers in a collection
		 */
		JavaRDD<Integer> mulNum = numRDD.filter(
				new Function<Integer, Boolean>(){
					private static final long serialVersionUID = 1L;

					public Boolean call(Integer num) throws Exception {
						return num % 2 == 0;
					}
			
		});
		return mulNum;
	}
	
	public static void printStr (JavaSparkContext sc) {
		List<String> str = Arrays.asList("hello you", "hello me", "hello word");
		JavaRDD<String> strRDD = sc.parallelize(str);
		JavaRDD<String> words = flatmap(strRDD);
		
		words.foreach(new VoidFunction<String>() {
			
			/**
			 * print
			 */
			private static final long serialVersionUID = 1L;

			public void call(String word) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(word);
				
			}
		});
	}
	
	private static JavaRDD<String> flatmap (JavaRDD<String> strRDD) {
		/**
		 * Split the text into words
		 */
		JavaRDD<String> wordRDD = strRDD.flatMap(new FlatMapFunction<String, String>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Iterable<String> call(String str) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(str.split(" "));
			}
			
		});
		
		return wordRDD;
		
	}
	
	private static void groupBykey (JavaSparkContext sc) {
		@SuppressWarnings("unchecked")
		List<Tuple2<String, Integer>> scores = Arrays.asList(
				new Tuple2<String, Integer>("c1", 99),
				new Tuple2<String, Integer>("c2", 89),
				new Tuple2<String, Integer>("c1", 79),
				new Tuple2<String, Integer>("c2", 100),
				new Tuple2<String, Integer>("c1", 95),
				new Tuple2<String, Integer>("c2", 84)
				);
		JavaPairRDD<String, Integer> scoresRDD = sc.parallelizePairs(scores);
		/*
		 * groupByKey
		 * Group grades into classes
		 */
		JavaPairRDD<String, Iterable<Integer>> groupeCores = scoresRDD.groupByKey();
		groupeCores.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>(){
			private static final long serialVersionUID = 1L;
			public void call (Tuple2<String, Iterable<Integer>> t) throws Exception {
				System.out.println("class: " + t._1 );
				Iterator<Integer> itr = t._2.iterator();
				while (itr.hasNext()) {
					System.out.println(itr.next());
				}
				System.out.println("----------------------");
				
			}
		});
		
		JavaPairRDD<String, Integer> reKey = scoresRDD.reduceByKey(new Function2<Integer, Integer, Integer>(){

			/**
			 * reduceByKey
			 * Total score of each class
			 */
			private static final long serialVersionUID = 1L;

			public Integer call(Integer t1, Integer t2) throws Exception {
				
				return t1 + t2;
			}
			
		});
		System.out.println("==========班级总分=========");
		reKey.foreach(new VoidFunction<Tuple2<String, Integer>>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public void call(Tuple2<String, Integer> t) throws Exception {

				System.out.println(t._1 + " 总分：" + t._2);
				System.out.println("------------------");
				
			}
			
		});
		
	}
	
	private static void sortByKey (JavaSparkContext sc) {
		@SuppressWarnings("unchecked")
		List<Tuple2<String, Integer>> scores = Arrays.asList(
				new Tuple2<String, Integer>("a", 99),
				new Tuple2<String, Integer>("b", 89),
				new Tuple2<String, Integer>("c", 79),
				new Tuple2<String, Integer>("d", 100),
				new Tuple2<String, Integer>("e", 95),
				new Tuple2<String, Integer>("f", 84)
				);
		JavaPairRDD<String, Integer> scoresRDD = sc.parallelizePairs(scores);
		JavaPairRDD<String, Integer> sortCores = scoresRDD.sortByKey(false);
		sortCores.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public void call(Tuple2<String, Integer> t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("=========分数排名==========");
				System.out.println(t._1 + " 分数：" + t._2);
			}
		});
	}
	
	private static void joinOrCogroup (JavaSparkContext sc) {
		/*
		 * 
		 */
		//初始化JavaPairRDD
		@SuppressWarnings("unchecked")
		List<Tuple2<Integer, String>> c1 = Arrays.asList(
				new Tuple2<Integer, String>(1, "a"),
				new Tuple2<Integer, String>(2, "b"),
				new Tuple2<Integer, String>(3, "c")
				);
		@SuppressWarnings("unchecked")
		List<Tuple2<Integer, Integer>> c2 = Arrays.asList(
				new Tuple2<Integer, Integer>(1, 99),
				new Tuple2<Integer, Integer>(2, 89),
				new Tuple2<Integer, Integer>(3, 79)
				);
		@SuppressWarnings("unchecked")
		List<Tuple2<Integer, Integer>> c3 = Arrays.asList(
				new Tuple2<Integer, Integer>(1, 99),
				new Tuple2<Integer, Integer>(2, 89),
				new Tuple2<Integer, Integer>(3, 79),
				new Tuple2<Integer, Integer>(3, 70),
				new Tuple2<Integer, Integer>(2, 87)
				);
		JavaPairRDD<Integer, String> cRDD1 = sc.parallelizePairs(c1);
		JavaPairRDD<Integer, Integer> cRDD2 = sc.parallelizePairs(c2);
		JavaPairRDD<Integer, Integer> cRDD3 = sc.parallelizePairs(c3);
		
		//join
		JavaPairRDD<Integer, Tuple2<String, Integer>> joinRDD = cRDD1.join(cRDD2); 
		joinRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>(){
			private static final long serialVersionUID = 1L;

			public void call(Tuple2<Integer, Tuple2<String, Integer>> t) throws Exception {
				System.out.println("学号：" + t._1);
				System.out.println("姓名："  + t._2._1);
				System.out.println("成绩："  + t._2._2);
			}
			
		});
		
		 // corgroup
		JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> coRDD = cRDD1.cogroup(cRDD3);
		coRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>(){
			/**
			 * corgroup
			 */
			private static final long serialVersionUID = 1L;

			public void call(
					Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t) throws Exception {
				System.out.println("学号：" + t._1);
				System.out.println("姓名："  + t._2._1);
				System.out.println("成绩："  + t._2._2);
			}
			
		});
	}

}
