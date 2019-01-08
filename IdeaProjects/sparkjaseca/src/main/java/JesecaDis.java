import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;


import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JesecaDis implements Serializable {
    private static final long serialVersionUID = -4892194648703458595L;
    public static String StringFilter(String str) {
   // 清除掉以下特殊字符
        String regEx = "[^a-zA-Z\\s]";
        Pattern matchsip = Pattern.compile(regEx);

        Matcher mp = matchsip.matcher(str);

        str = mp.replaceAll("");
        return str;
    }

    //求两个字符串数组的并集，利用set的元素唯一性
    public static String[] union(String[] arr1, String[] arr2) {
        Set<String> set = new HashSet<String>();
        for (String str : arr1) {
            set.add(str);
        }
        for (String str : arr2) {
            set.add(str);
        }
        String[] result = {};
        return set.toArray(result);
    }

    //求两个数组的交集
    public static String[] intersect(String[] arr1, String[] arr2) {
        Map<String, Boolean> map = new HashMap<String, Boolean>();
        LinkedList<String> list = new LinkedList<String>();
        for (String str : arr1) {
            if (!map.containsKey(str)) {
                map.put(str, Boolean.FALSE);
            }
        }
        for (String str : arr2) {
            if (map.containsKey(str)) {
                map.put(str, Boolean.TRUE);
            }
        }

        for (Map.Entry<String, Boolean> e : map.entrySet()) {
            if (e.getValue().equals(Boolean.TRUE)) {
                list.add(e.getKey());
            }
        }

        String[] result = {};
        return list.toArray(result);
    }
    public static void main(String[] args) {
        final int testnum = 100;
        String inputFile = "hdfs://hserver1:9000/test/input/documentdata.txt"; // Should be some file on your system
        String input1File = "hdfs://hserver1:9000/test/input1/documentdata.txt";
        String outFile = "hdfs://hserver1:9000/test/output/document";
        SparkConf conf = new SparkConf().setAppName("jeccsa Application");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(inputFile, 20).cache();
        JavaRDD<String> log1data = sc.textFile(input1File, 20).cache();

        List<String> heiheihei = new ArrayList<String>();
        for(int i = 0; i< testnum; i++){
            heiheihei.add("fa tmp tr");
            heiheihei.add("fa tmp trq");
            heiheihei.add("fa tmp trwqe");
        }

        List<String> heiheihei1 = new ArrayList<String>();
        for(int i = 0; i< testnum; i++){
            heiheihei1.add("fa tmp tr");
            heiheihei1.add("fa tmp trs");
            heiheihei1.add("fa tmp trdsf");
        }
        /*JavaRDD<String> logData = sc.parallelize(heiheihei, 20).cache();
        JavaRDD<String> log1data = sc.parallelize(heiheihei1, 20).cache();*/

        JavaRDD<String> newRDD = logData.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                s.replace("\n", "");
                s.replace("\t", "");
                s = StringFilter(s);
                return Arrays.asList(s).iterator();
            }
        }).cache();

        JavaRDD<String> new1RDD = log1data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                s.replace("\n", "");
                s.replace("\t", "");
                s = StringFilter(s);
                return Arrays.asList(s).iterator();
            }
        }).cache();

        /*List<JavaRDD<String>> RDDlist = new ArrayList<JavaRDD<String>>();
        for(String JavaString : new1RDD.collect()) {
            String [] splits = JavaString.split(" ");
            RDDlist.add(sc.parallelize(Arrays.asList(splits)));
        }*/

        List<Long> resLists = new ArrayList<Long>();
        for (final String stringJava1 : newRDD.collect()) {
            long temps = new1RDD.filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String s) throws Exception {
                    String tempStr = String.valueOf(stringJava1);
                    String [] splits = tempStr.split(" ");
                    String [] splits1 = s.split(" ");
/*                    System.out.println("\n\n\n\ndocument splits:" + tempStr);
                    System.out.println("\n\n\n\n");
                    System.out.println("\n\n\n\ndocument splits:" + s);
                    System.out.println("\n\n\n\n");*/
                    String[] unions = union(splits1, splits);
                    String[] cuts = intersect(splits1, splits);
                    double second = (double) cuts.length;
                    /*System.out.println("\n\n\n\ndocument second:" + second);
                    System.out.println("\n\n\n\n");*/


                    int sum = unions.length;
                    double first = (double) sum;
                    /*System.out.println("\n\n\n\ndocument sum:" + sum);
                    System.out.println("\n\n\n\n");*/
                    if (sum != 0) {
                        double res = second / first;
/*                        System.out.println("\n\n\n\ndocument sum:" + res);
                        System.out.println("\n\n\n\n");*/
                        return res - 0.8 > 0;
                    }
                    return false;
                }
            }).count();
            resLists.add(temps);
        }

        /*String [] splits1 = stringJava1.split(" ");
        final JavaRDD<String> stringJavaRDD = sc.parallelize(Arrays.asList(splits1));
        for (JavaRDD<String> stringJavaRDD1 : RDDlist) {
            long temps = 0;
            long same = stringJavaRDD.union(stringJavaRDD1).count();
            double first  = (double)stringJavaRDD.intersection(stringJavaRDD1).count();
            double sencond = (double) same;
            if (same != 0) {
                double res = (sencond - first) / sencond;
                if(res > 0.8) {
                    temps = 1;
                }
            }
            resLists.add(temps);
        }*/

        long res = 0;
        for (long key : resLists) {
            res += key;
        }

        System.out.println("\n\n\n\ndocument amount:" + res);
        System.out.println("\n\n\n\n");
        JavaRDD<Long> reslong = sc.parallelize(Arrays.asList(res));
        reslong.saveAsTextFile(outFile);

        sc.close();

    }

}
