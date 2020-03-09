package com.huangbing.spark.opterators;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MapPartitionsWithIndexJavaDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]").setAppName("MapPartitionsWithIndexJavaDemo");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = jsc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));

        //rdd
        JavaRDD<String> rdd2 = rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer v1, Iterator<String> v2) throws Exception {
                ArrayList<String> res = new ArrayList<>();
                while (v2.hasNext()){
                    String next = v2.next();
                    res.add("regionNo:"+v1+"  value="+next);
                }
                return res.iterator();
            }
        }, true);

        rdd2.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        //pairRDD
        JavaPairRDD<String, Integer> pairRdd = rdd.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return Tuple2.apply(s, 1);
            }
        });

        JavaRDD<Tuple2<String, Integer>> rdd3 = pairRdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<String, Integer>>, Iterator<Tuple2<String, Integer>>>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(Integer v1, Iterator<Tuple2<String, Integer>> v2) throws Exception {
                ArrayList<Tuple2<String, Integer>> res = new ArrayList<>();
                while (v2.hasNext()) {
                    Tuple2<String, Integer> next = v2.next();
                    res.add(Tuple2.apply(next._1 + " " + v1, next._2));
                }
                return res.iterator();
            }
        }, true);

        List<Tuple2<String,Integer>> collected = rdd3.collect();
        for(Tuple2<String,Integer> tuple2 : collected){
            System.out.println(tuple2);
        }


        jsc.stop();



    }
}
