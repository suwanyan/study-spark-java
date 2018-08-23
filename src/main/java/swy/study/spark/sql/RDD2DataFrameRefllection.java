package swy.study.spark.sql;


import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class RDD2DataFrameRefllection {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("RDD2DataFrameRefllection")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		JavaRDD<String> lines = sc.textFile(
				"E://swy//resource//workspace-neno//spark-study-java//txt//students.txt");
		
		JavaRDD<Student> students = lines.map(new Function<String, Student>(){

			private static final long serialVersionUID = 1L;

			public Student call(String line) throws Exception {
				String[] lineSpited = line.split(",");
				Student stu = new Student();
				stu.setId(Integer.valueOf(lineSpited[0]));
				stu.setName(lineSpited[1]);
				stu.setAge(Integer.valueOf(lineSpited[2]));
				return stu;
			}
		});
		
		//使用反射方式，将RDD转换为DataFrame
		DataFrame studentDF = sqlContext.createDataFrame(students, Student.class);
		
		//注册临时表
		studentDF.registerTempTable("students");
		
		//针对临时表执行Sql语句（查询年龄小于18岁的学生）
		//JavaBean必须实现Serializable接口
		DataFrame tenagerDF = sqlContext.sql("select * from students where age<=18");
		
		 //将查询出来的数据再次转换为RDD
		JavaRDD<Row> tenagerRDD = tenagerDF.javaRDD();
		
		//将RDD数据映射为Student格式
		JavaRDD<Student> tenagerStudents = tenagerRDD.map(new Function<Row, Student>(){

			private static final long serialVersionUID = 1L;

			//顺序和期望的可能不一样
			public Student call(Row row) throws Exception {
				Student stu = new Student();
				stu.setId(row.getInt(1));
				stu.setName(row.getString(2));
				stu.setAge(row.getInt(0));
				return stu;
			}
		});
		
		//打印
		List<Student> studentList = tenagerStudents.collect();
		for (Student stu : studentList) {
			System.out.println("学号： " + stu.getId());
			System.out.println("姓名： " + stu.getName());
			System.out.println("年龄： " + stu.getAge()); 
		}
	}	
}
