package driver;

import kvtype.FloatAndFloat;
import kvtype.FloatAndLong;
import mr.SingleLinearRegressionErrorMapper;
import mr.SingleLinearRegressionErrorReducer;
import mr.SingleLinearRegressionErrorReducer2;
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

/**
 * 第四步：使用第三步中求得的theta值来求解全局误差
 *
 * Created by fanzhe on 2016/10/23.
 */
public class LastLinearRegressionError extends Configured implements Tool
{
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration() , new LastLinearRegressionError(),args);
    }
    public int run(String[] args) throws Exception {
//        if(args.length!= 4){
//            System.err.println("Usage : driver.LastLinearRegressionError <input> <output> <theta_path> <splitter> " );
//            System.exit(-1);
//        }
        Configuration conf = getConf();


        conf.set(Utils.SINGLE_LINEAR_PATH,PathUtil.CAL_ERROR_THETA_PATH);
        conf.set(Utils.LINEAR_SPLITTER,PathUtil.SPLITTER);

        Job job = Job.getInstance(conf,"Last Linear Regression Error");

        job.setMapperClass(SingleLinearRegressionErrorMapper.class);
        job.setReducerClass(SingleLinearRegressionErrorReducer2.class);//

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatAndLong.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, Utils.str2Path(PathUtil.CAL_ERROR_DATA_PATH));
        FileOutputFormat.setOutputPath(job, Utils.str2Path(PathUtil.CAL_ERROR_OUTPUT_PATH));
        Utils.delete(conf, PathUtil.CAL_ERROR_OUTPUT_PATH);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
