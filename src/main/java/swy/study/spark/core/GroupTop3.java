package swy.study.spark.core;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 分组取top3
 * @author swy
 *
 */

public class GroupTop3 {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("ActionOperation")
				.setMaster("local");
				//.setMaster("spark://192.168.43.124:7077");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile("E://swy//resource//workspace-neno//spark-study-java//txt//score.txt");
		
		JavaPairRDD<String, Integer> pairs = lines.mapToPair(
				new PairFunction<String, String, Integer>(){
					
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(String s) throws Exception {
						String[] lineSpited = s.split(" ");
						return new Tuple2<String, Integer>(lineSpited[0], Integer.valueOf(lineSpited[1]));
					}
			
		});
		
		//将成绩按班级分组
		JavaPairRDD<String, Iterable<Integer>> groupPairs = pairs.groupByKey(); 
		
		JavaPairRDD<String, Iterable<Integer>> top3Sores = groupPairs.mapToPair(
				new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>(){
					
					private static final long serialVersionUID = 1L;
					
					public Tuple2<String, Iterable<Integer>> call (
							Tuple2<String, Iterable<Integer>> classSorces) throws Exception {
						Integer[] top3 = new Integer[3];
						
						String className = classSorces._1;
						Iterator<Integer> scores = classSorces._2.iterator();
						
						while (scores.hasNext()) {
							Integer score = scores.next();
							
							for (int i = 0; i<3; i++) {
								if (top3[i] == null) {
									top3[i] = score;
									break;
								} else if (score > top3[i]) {
									int tmp = top3[i];
									top3[i] = score;
									
									if (i < top3.length - 1) {
										top3[i+1] = tmp;
									}
									break;
								}
							}
						}
						return new Tuple2<String, 
								Iterable<Integer>>(className, Arrays.asList(top3));
					}
		});
		
		top3Sores.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>(){
			private static final long serialVersionUID = 1L;
			
			public void call(Tuple2<String, Iterable<Integer>> v) throws Exception {
				System.out.println("班级：" + v._1);
				System.out.println("前三名：" + v._2);
			}
			
		});
		
		sc.close();
	}
}


