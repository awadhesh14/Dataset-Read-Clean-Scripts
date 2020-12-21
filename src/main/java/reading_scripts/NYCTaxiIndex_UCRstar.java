package reading_scripts;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.spatialRDD.PointRDD;

import java.util.Random;

public class NYCTaxiIndex_UCRstar {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("GeoSparkRunnableExample").setMaster("local[*]");
        conf.set("spark.serializer", KryoSerializer.class.getName());
        //conf.set("spark.kryo.registrator", GeoSparkVizKryoRegistrator.class.getName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        String resourceFolder = System.getProperty("user.dir")+"/src/main/resources/";

        String PointRDDInputLocation  = resourceFolder + "NYCTaxiIndex_UCRstar_small.csv";
        String outputFile = resourceFolder + "NYCTaxiIndex_UCRstar_small" + new Random().nextInt();

        FileDataSplitter PointRDDSplitter = FileDataSplitter.WKT;
        Integer PointRDDNumPartitions = 1;
        Integer PointRDDStartOffset = 0;
        //Integer PointRDDEndOffset = -1;

        PointRDD NYCTaxi = new PointRDD(sc,PointRDDInputLocation,PointRDDStartOffset,PointRDDSplitter,true,PointRDDNumPartitions);
        NYCTaxi.saveAsGeoJSON(outputFile);

    }
}
