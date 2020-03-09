package com.huangbing.spark.opterators;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

public class SampleJavaDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
//        Properties prop = new Properties();
//        try {
//            prop.load(SampleJavaDemo.class.getResourceAsStream("xxx.properties"));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        String master = prop.getProperty("spark.master");
        conf.setMaster("local[*]").setAppName("SampleJavaDemo");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<Integer> numbers = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 10, 11));

        JavaRDD<Integer> sampled = numbers.sample(false, 0.3, 105L);

        sampled.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        jsc.stop();
    }
}
