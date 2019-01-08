import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.spark_project.guava.collect.Lists;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by hadoop on 17-4-4.
 */
public class SparkWordCount implements Serializable {
    private static final long serialVersionUID = 1L;

    static double getDistance(double longt1, double lat1, double longt2, double lat2){

        final double PI = 3.14159265358979323; //圆周率

        final double R = 6371229;
        double x,y, distance;

        x=(longt2-longt1)*PI*R*Math.cos( ((lat1+lat2)/2)*PI/180)/180;

        y=(lat2-lat1)*PI*R/180;

        distance=Math.hypot(x,y);

        return distance;

    }



    public static void main(String[] args) throws Exception{
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(30);
        final int testnum = 10;
        String inputFile = "hdfs://hserver1:9000/test/input/poidata.txt"; // Should be some file on your system
        String input1File = "hdfs://hserver1:9000/test/input1/poidata.txt";
        String outFile = "hdfs://hserver1:9000/test/output/poitest";
        SparkConf conf = new SparkConf().setAppName("poitest Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
/*        JavaRDD<String> logData = sc.textFile(inputFile, 100).cache();
        JavaRDD<String> log1Data = sc.textFile(input1File, 100).cache();*/

        List<String> heiheihei = new ArrayList<String>();
        for(int i = 0; i< testnum; i++){
            heiheihei.add("0 123 86");
            heiheihei.add("0 123 86");
            heiheihei.add("0 123 86");
        }

        List<String> heiheihei1 = new ArrayList<String>();
        for(int i = 0; i< testnum; i++){
            heiheihei1.add("0 116.61725 40.21235");
            heiheihei1.add("0 124 86");
            heiheihei1.add("0 123 86");
        }

        JavaRDD<String> logData = sc.parallelize(heiheihei).cache();
        JavaRDD<String> log1Data = sc.parallelize(heiheihei1).cache();


        JavaRDD<String> newlogData = logData.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String [] splits = s.split(" ");
                String result = splits[1] + " " + splits[2];
                //System.out.println("\n\n\nnewLogData: " + result);
                return Arrays.asList(result).iterator();
            }
        });

        final JavaRDD<String> newlog1Data = log1Data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String [] splits = s.split(" ");
                String result = splits[1] + " " + splits[2];
                //System.out.println("\n\n\nnewLog1Data: " + result);
                return Arrays.asList(result).iterator();
            }
        });

        final List<Long> reslists = new Vector<Long>();

        for (final String temp : newlogData.collect()) {
            Thread thread = new Thread(new Runnable()  {
                @Override
                public void run() {
                    long temps = newlog1Data.filter(new Function<String, Boolean>() {
                        @Override
                        public Boolean call(String s) throws Exception {
                            String strtrmp = String.valueOf(temp);
                            String[] split1 = strtrmp.split(" ");
                            String[] split2 = s.split(" ");
                            double dis = getDistance(Double.valueOf(split1[0]), Double.valueOf(split1[1]), Double.valueOf(split2[0]), Double.valueOf(split2[1]));
                            //System.out.println("\n\n\ndis is :" + dis);
                            return dis < 1000;
                        }
                    }).count();
                    reslists.add(temps);
                }
            });
        }


       /* JavaPairRDD<String, String> unions = newlogData.cartesian(newlog1Data);

        *//*for(Tuple2<String, String> tuple2: unions.collect()) {
            System.out.println("\n\n\nres: " + tuple2._1() + " " + tuple2._2());
        }*//*

        long res = unions.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
                String[] split1 = stringStringTuple2._1().split(" ");
                String[] split2 = stringStringTuple2._2().split(" ");
                double dis = getDistance(Double.valueOf(split1[0]), Double.valueOf(split1[1]), Double.valueOf(split2[0]), Double.valueOf(split2[1]));
                //System.out.println("\n\n\ndis is :" + dis);
                return dis < 1000;
            }
        }).count();*/

        long res = 0;
        for (long key : reslists) {
            res += key;
        }

        System.out.println("\n\n\ndistance < 1km total:" + res);
        JavaRDD<Long> reslong = sc.parallelize(Arrays.asList(res));
        reslong.saveAsTextFile(outFile);

        sc.stop();

    }
}