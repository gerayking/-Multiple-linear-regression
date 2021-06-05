package mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.Utils;

import java.io.IOException;

/**
 * 每个mapper的map函数更新 theta0 和 theta1的公式为
 * theta0 = theta0 - alpha *( h(x) - y) * x
 * theta1 = theta1 - alpha *( h(x) - y) * x
 *
 * Created by fanzhe on 2016/10/23.
 */
public class LinearRegressionMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
    private float alpha = 0.01f;
    private float[] theta;
    private int dim = 0;
    private String splitter = ",";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        alpha = context.getConfiguration().getFloat(Utils.LINEAR_ALPHA,0.01f);
        theta = new float[100];
        theta[0]=context.getConfiguration().getFloat(Utils.LINEAR_THETA0,1.0f);
        for(int i=1;i<100;i++)theta[i]=0.0f;
        splitter = context.getConfiguration().get(Utils.LINEAR_SPLITTER,",");
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        float[] xy = Utils.str2float(value.toString().split(splitter));
        dim = xy.length;
        float y = xy[xy.length-1];
//        // 同步更新 theta0 and theta1 and theta2
        double h = theta[0] - y;
        for(int i=1;i<dim;i++){
            h+=theta[i]*xy[i-1];
        }
        for(int i=0;i<dim;i++){
            if(i==0)theta[i]-=alpha*h*1;
            else theta[i] -= alpha*h*xy[i-1];
        }
    }

    private Text thetaresult = new Text();
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String result = "";
        for(int i=0;i<dim;i++){
            if(i!=dim-1) result += theta[i] + splitter;
            else result += theta[i];
        }
        thetaresult.set(result);
        context.write(thetaresult,NullWritable.get());
    }
}
