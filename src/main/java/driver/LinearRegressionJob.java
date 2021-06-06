package driver;

import mr.LinearRegressionMapper;
import mr.ShuffleMapper;
import mr.ShuffleReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import util.Utils;

import java.nio.channels.FileLock;

/**
 * 第二步：使用随机梯度下降（SGD）求解每个mapper的最优函数
 * 此方法只针对只有一个全局最优解的情况（如一元一次线性回归）
 * 因为如果每个mapper得到不同的参数Theta，那么在Reducer端如何整合就会有不同的方式
 * Created by fanzhe on 2016/10/23.
 */
public class LinearRegressionJob extends Configured implements Tool
{
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration() , new LinearRegressionJob(),args);
    }
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.setLong("mapreduce.input.fileinputformat.split.maxsize",10000000L);// 获取多个mapper；
        String[] parameter = PathUtil.LINEAER_THETA_PARAMETER.split(";");
        conf.set(Utils.LINEAR_ALPHA,parameter[parameter.length-1]);
        Job job = Job.getInstance(conf,"Linear Regression Job");

        job.setMapperClass(LinearRegressionMapper.class);
//        job.setReducerClass(LinearRegressionReducer.class);// 不使用mapper即可
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, Utils.str2Path(PathUtil.DATA_PATH));
        FileOutputFormat.setOutputPath(job, Utils.str2Path(PathUtil.LINEAER_REGRESSION_OUTPUT_PATH));
        if (job.waitForCompletion(true)){
            SingleLinearRegressionError.main(null);
        }
        return 1;
    }
}
