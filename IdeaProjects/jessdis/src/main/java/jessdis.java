import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings("unused")
public class jessdis {
    public static int acount = 18874;

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
    public static int temp = 0;
    public static List<String> templist = new Vector<String>();
    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text>
    {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            FileSplit split = (FileSplit) context.getInputSplit();
            String name = split.getPath().getName();
            String newstr = StringFilter(value.toString());
            if(name.equals("documentdata.txt")) {
                IntWritable one = new IntWritable(Integer.valueOf(temp++));
                context.write(one, new Text(newstr));
            } else {
                templist.add(newstr);
            }
        }
    }


    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println(key);
            int sum = 0;

            for (Text temp : values) {
                String [] splits = temp.toString().split(" ");
                for(String val : templist) {
                    String[] splits1 = val.toString().split(" ");
/*                    System.out.println("\n\n\n\ndocument splits:" + tempStr);
                    System.out.println("\n\n\n\n");
                    System.out.println("\n\n\n\ndocument splits:" + s);
                    System.out.println("\n\n\n\n");*/
                    String[] unions = union(splits1, splits);
                    String[] cuts = intersect(splits1, splits);
                    double second = (double) cuts.length;
                    /*System.out.println("\n\n\n\ndocument second:" + second);
                    System.out.println("\n\n\n\n");*/


                    int sums = unions.length;
                    double first = (double) sums;
                    /*System.out.println("\n\n\n\ndocument sum:" + sum);
                    System.out.println("\n\n\n\n");*/
                    double res = 0;
                    if (sums != 0) {
                        res = second / first;
/*                        System.out.println("\n\n\n\ndocument sum:" + res);
                        System.out.println("\n\n\n\n");*/
                    }
                    if (res > 0.8) {
                        sum += 1;
                    }
                }
            }

            result.set(sum);
            context.write(new Text("res"), result);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();//程序里，只需写这么一句话，就会加载到hadoop的配置文件了
        //Configuration类代表作业的配置，该类会加载mapred-site.xml、hdfs-site.xml、core-site.xml等配置文件。

        //删除已经存在的输出目录
        Path mypath = new Path("hdfs://hserver1:9000/documentout");//输出路径
        FileSystem hdfs = mypath.getFileSystem(conf);//程序里，只需写这么一句话，就可以获取到文件系统了。

        if (hdfs.isDirectory(mypath)) {
            hdfs.delete(mypath, true);
        }

        //job对象指定了作业执行规范，可以用它来控制整个作业的运行。
        Job job = Job.getInstance();
        job.setJarByClass(jessdis.class);//我们在hadoop集群上运行作业的时候，要把代码打包成一个jar文件，然后把这个文件
        //传到集群上，然后通过命令来执行这个作业，但是命令中不必指定JAR文件的名称，在这条命令中通过job对象的setJarByClass（）中传递一个主类就行，hadoop会通过这个主类来查找包含它的JAR文件。

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);




        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job, new Path(
                "hdfs://hserver1:9000/test/input1/documentdata1.txt"));
        FileInputFormat.addInputPath(job, new Path(
                "hdfs://hserver1:9000/test/input/documentdata.txt"));//FileInputFormat.addInputPath（）指定的这个路径可以是单个文件、一个目录或符合特定文件模式的一系列文件。
        //从方法名称可以看出，可以通过多次调用这个方法来实现多路径的输入。
        FileOutputFormat.setOutputPath(job, new Path(
                "hdfs://hserver1:9000/documentout"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
