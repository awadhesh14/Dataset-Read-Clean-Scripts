import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.spatialRDD.LineStringRDD;

import java.util.Random;

public class OSM2015_UCRStar_Road_Network {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("GeoSparkRunnableExample").setMaster("local[*]");
        conf.set("spark.serializer", KryoSerializer.class.getName());
        //conf.set("spark.kryo.registrator", GeoSparkVizKryoRegistrator.class.getName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        String resourceFolder = System.getProperty("user.dir")+"/src/main/resources/";

        String LineRDDInputLocation  = resourceFolder + "OSM2015_UCRstar_road-network_small.csv";
        String outputFile = resourceFolder + "OSM2015_UCRstar_road-network_small" + new Random().nextInt();

        FileDataSplitter LineRDDSplitter = FileDataSplitter.WKT;
        Integer LineRDDNumPartitions = 1;
        Integer LineRDDStartOffset = 0;
        Integer LineRDDEndOffset = -1;

        LineStringRDD OSM2015RoadNetwork = new LineStringRDD(sc,LineRDDInputLocation,LineRDDStartOffset,LineRDDEndOffset,LineRDDSplitter,true,LineRDDNumPartitions );
        OSM2015RoadNetwork.saveAsGeoJSON(outputFile);
    }
}
