package driver;

public class PathUtil {
    public static String SPLITTER = ",";
    public static String DATA_PATH = "./src/main/resources/foo.csv";
    public static String SHUFFER_OUTPUT_PATH = "./src/main/resources/shuffer";
    public static int SHUFFER_PATTEN = 1;
    public static String LINEAER_REGRESSION_DATA_PATH = PathUtil.DATA_PATH;
    public static String LINEAER_REGRESSION_OUTPUT_PATH = "./src/main/resources/lineraRegerssion";
    public static String LINEAER_THETA_PARAMETER = "1;0;0;0.0000001";
    public static String COMBINE_DATA_INPUT_PATH = PathUtil.DATA_PATH;
    public static String COMBINE_OUTPUT_PATH = "./src/main/resources/CombineTheta";
    public static String COMBINE_THETA_INPUT_PATH = "./src/main/resources/lineraRegerssion";
    public static String COMBINE_MODE = "weight";//<average|weight>
    public static String CAL_ERROR_DATA_PATH = PathUtil.DATA_PATH;
    public static String CAL_ERROR_OUTPUT_PATH = "./src/main/resources/Error";
    public static String CAL_ERROR_THETA_PATH = "./src/main/resources/CombineTheta/part-r-00000";
}
