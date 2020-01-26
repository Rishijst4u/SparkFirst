package com.spark;


import com.model.TestPojo;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;


public class SparkFirst {
    public static void main(String args[]) throws InterruptedException {

        Logger.getLogger("org").setLevel(Level.OFF);
        SparkConf sparkConf=new SparkConf().setAppName("My First Spark Application").setMaster("local[*]");
        JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);

        List<Integer> intlist= Arrays.asList(1,2,3,4,5,6,7,8,9);
        JavaRDD listrdd=javaSparkContext.parallelize(intlist);
        System.out.println("Full List");
        System.out.println(listrdd.collect());
        JavaRDD evenList=listrdd.filter( x->
        {
            if( Integer.parseInt(x+"")%2 == 0)
                return true;
            return false;
        });

        System.out.println("Even List");
        System.out.println(evenList.collect());
        JavaRDD oddList=listrdd.filter( x->
        {
            if( Integer.parseInt(x+"")%2 == 1)
                return true;
            return false;
        });

        System.out.println("Odd List");
        System.out.println(oddList.collect());

    }
}
