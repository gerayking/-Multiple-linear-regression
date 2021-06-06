package mr;

import kvtype.FloatAndFloat;
import kvtype.FloatAndLong;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.Utils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * 得到每个theta的参数值的全局误差
 * Created by fanzhe on 2016/10/23.
 */
public class SingleLinearRegressionErrorMapper extends Mapper<LongWritable,Text, Text, FloatAndLong> {

    private String thetaPath = null;
    private String splitter = ",";
    private  List<float[]> thetas = new ArrayList<float[]>();
    private float[] thetaErrors = null;
    private long [] thetaNumbers = null;
    @Override
    protected void setup(Context context) throws IOException {
        thetaPath = context.getConfiguration().get(Utils.SINGLE_LINEAR_PATH,null);
        splitter = context.getConfiguration().get(Utils.LINEAR_SPLITTER,",");
        if(thetaPath == null) {System.err.println("theta path exception");System.exit(-1);}
        FileStatus[] files = null;

        try {
            files = FileSystem.get(new URI("hdfs://localhost:9000"),context.getConfiguration()).listStatus(Utils.str2Path(thetaPath), new PathFilter() {
                public boolean accept(Path path) {
                    if(path.toString().contains(Utils.MAPPER_OUTPUT_PREFIX)) return true ;
                    return false;
                }
            });
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        for(FileStatus file : files){
            thetas.add(Utils.readFromOneTheatFile(context.getConfiguration(), file.getPath(), splitter));
        }
        thetaErrors = new float[thetas.size()];
        thetaNumbers = new long[thetas.size()];
        System.out.println("thetas array size :"+thetas.size());
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) {
        float[] xy = Utils.str2float(value.toString().split(splitter));
        int dim = xy.length;
        float y =xy[xy.length-1];
        for(int i =0;i<thetas.size();i++){
            // error = (theta0 + theta1 * x +theta2 * x1 - y) ^2
            double h = thetas.get(i)[0] - y;
            for(int j=0;j<dim-1;j++){
                h+=xy[j] * thetas.get(i)[j+1];
            }
            thetaErrors[i] += h*h;
            thetaNumbers[i]+= 1;
        }
    }

    private FloatAndLong floatAndLong = new FloatAndLong();
    private Text theta = new Text();
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(int i =0;i<thetas.size() ;i++){
            String thetaString = "";
            for(int j=0;j<thetas.get(i).length;j++){
                if(j!=thetas.get(i).length-1) thetaString+=thetas.get(i)[j]+splitter;
                else thetaString+=thetas.get(i)[j];
            }
            theta.set(thetaString);
            floatAndLong.set(thetaErrors[i],thetaNumbers[i]);
            context.write(theta,floatAndLong);
        }
    }
}
