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
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;

public class poidis {
    public static int acount = 338024;
    public static List<String> templist = new Vector<String>();

    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text>
    {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            FileSplit split = (FileSplit) context.getInputSplit();
            String name = split.getPath().getName();
            String [] splits = value.toString().split(" ");
            if(name.equals("poidata.txt")) {
                IntWritable one = new IntWritable(Integer.valueOf(splits[0]));
                context.write(one, new Text(splits[1]+ " " + splits[2]));
            } else {
                templist.add(splits[1]+ " " + splits[2]);
            }
        }
    }

    public static double getDistance(double longt1, double lat1, double longt2, double lat2){

        final double PI = 3.14159265358979323; //圆周率

        final double R = 6371229;
        double x,y, distance;

        x=(longt2-longt1)*PI*R*Math.cos( ((lat1+lat2)/2)*PI/180)/180;

        y=(lat2-lat1)*PI*R/180;

        distance=Math.hypot(x,y);

        return distance;

    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, IntWritable, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println(key);
            int sum = 0;
            for(Text temp : values) {
               String [] splits = temp.toString().split(" ");
               for (String val : templist) {
                      String[] splits1 = val.toString().split(" ");
                      double dis = getDistance(Double.valueOf(splits1[0]), Double.valueOf(splits1[1]), Double.valueOf(splits[1]), Double.valueOf(splits[2]));
                      if (dis < 1000) {
                        sum += 1;
                      }
               }
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();//程序里，只需写这么一句话，就会加载到hadoop的配置文件了
        //Configuration类代表作业的配置，该类会加载mapred-site.xml、hdfs-site.xml、core-site.xml等配置文件。

        //删除已经存在的输出目录
        Path mypath = new Path("hdfs://hserver1:9000/hdfsOutput");//输出路径
        FileSystem hdfs = mypath.getFileSystem(conf);//程序里，只需写这么一句话，就可以获取到文件系统了。

        if (hdfs.isDirectory(mypath)) {
            hdfs.delete(mypath, true);
        }

        //job对象指定了作业执行规范，可以用它来控制整个作业的运行。
        Job job = Job.getInstance();
        job.setJarByClass(poidis.class);//我们在hadoop集群上运行作业的时候，要把代码打包成一个jar文件，然后把这个文件
        //传到集群上，然后通过命令来执行这个作业，但是命令中不必指定JAR文件的名称，在这条命令中通过job对象的setJarByClass（）中传递一个主类就行，hadoop会通过这个主类来查找包含它的JAR文件。

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);




        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job, new Path(
                "hdfs://hserver1:9000/test/input/poidata.txt"));//FileInputFormat.addInputPath（）指定的这个路径可以是单个文件、一个目录或符合特定文件模式的一系列文件。
        //从方法名称可以看出，可以通过多次调用这个方法来实现多路径的输入。
        FileInputFormat.addInputPath(job, new Path(
                "hdfs://hserver1:9000/test/input1/poidata1.txt"));//FileInputFormat.addInputPath（）指定的这个路径可以是单个文件、一个目录或符合特定文件模式的一系列文件。
        //从方法名称可以看出，可以通过多次调用这个方法来实现多路径的输入。
        FileOutputFormat.setOutputPath(job, new Path(
                "hdfs://hserver1:9000/poiout"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
