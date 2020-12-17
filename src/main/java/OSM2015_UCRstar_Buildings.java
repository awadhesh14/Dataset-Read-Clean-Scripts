import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;

import java.util.Random;

public class OSM2015_UCRstar_Buildings {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("GeoSparkRunnableExample").setMaster("local[*]");
        conf.set("spark.serializer", KryoSerializer.class.getName());
        //conf.set("spark.kryo.registrator", GeoSparkVizKryoRegistrator.class.getName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        String resourceFolder = System.getProperty("user.dir")+"/src/main/resources/";

        String PolygonRDDInputLocation = resourceFolder + "OSM2015_UCRstar_buildings_small.csv";
        String outputFile = resourceFolder + "OSM2015_UCRstar_buildings_small" + + new Random().nextInt();

        FileDataSplitter PolygonRDDSplitter = FileDataSplitter.WKT;
        Integer PolygonRDDNumPartitions = 1;
        Integer PolygonRDDStartOffset = 0;
        Integer PolygonRDDEndOffset = -1;

        PolygonRDD OSM15Building = new PolygonRDD(sc,PolygonRDDInputLocation,PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true,PolygonRDDNumPartitions);
        OSM15Building.saveAsGeoJSON(outputFile);
    }
}
