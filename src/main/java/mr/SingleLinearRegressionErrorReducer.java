package mr;

import kvtype.FloatAndFloat;
import kvtype.FloatAndLong;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by fanzhe on 2016/10/23.
 */
public class SingleLinearRegressionErrorReducer extends Reducer<Text,FloatAndLong,Text,NullWritable> {
    List<float[]> theta_error = new ArrayList<float[]>();
    String method = "average";
    Logger logger = LoggerFactory.getLogger(getClass());
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        method = context.getConfiguration().get(Utils.SINGLE_REDUCER_METHOD);
    }

    @Override
    protected void reduce(Text key, Iterable<FloatAndLong> values, Context context){
        float sumF = 0.0f;
        long sumL = 0L ;
        for(FloatAndLong value:values){
            sumF += value.getSumFloat();
            sumL += value.getSumLong();
        }
        String[] keys = key.toString().split(",");
        float[] keysvalue = new float[keys.length+1];
        for(int i=0;i<keys.length;i++){
            keysvalue[i] = Float.parseFloat(keys[i]);
        }
        keysvalue[keys.length] = (float)Math.sqrt((double)sumF / sumL);
        theta_error.add(keysvalue);
        logger.info("theta:{}, error:{}", new Object[]{key.toString(),Math.sqrt(sumF/sumL)});
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 如何加权？
        // 方式1：如果误差越小，那么说明权重应该越大；
        // 方式2：直接平均值
        int dim = theta_error.get(0).length;
        float [] theta_all = new float[dim];
        if("average".equals(method)){
            for(int i=0;i< theta_error.size();i++){
                for(int j=0;j<dim;j++){
                    theta_all[j] += theta_error.get(i)[j];
                }
            }
            for(int i=0;i<dim;i++){
                theta_all[i] /= theta_error.size();
            }
        } else {
            float sumErrors = 0.0f;
            for(float[] d:theta_error){
                sumErrors += 1/d[dim-1];
            }
            for(float[] d: theta_error){
                for(int j=0;j<dim-1;j++){
                    theta_all[j] += d[j] * 1 / d[dim-1] / sumErrors;
                }
            }
        }
        String thetavalue = "";
        for(int i=0;i<dim-1;i++){
            if(i!=dim-2)thetavalue+=theta_all[i]+",";
            else thetavalue+=theta_all[i];
        }
        context.write(new Text(thetavalue),NullWritable.get());
    }
}
