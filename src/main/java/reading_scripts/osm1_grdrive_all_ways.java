package reading_scripts;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.spatialRDD.LineStringRDD;

import java.util.Random;

public class osm1_grdrive_all_ways {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("GeoSparkRunnableExample").setMaster("local[*]");
        conf.set("spark.serializer", KryoSerializer.class.getName());
        //conf.set("spark.kryo.registrator", GeoSparkVizKryoRegistrator.class.getName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        String resourceFolder = System.getProperty("user.dir")+"/src/main/resources/";

        String LineRDDInputLocation  = resourceFolder + "osm1_gdrive_all_ways_small.tsv";
        String outputFile = resourceFolder + "osm1_gdrive_all_ways_small" + new Random().nextInt();

        FileDataSplitter LineRDDSplitter = FileDataSplitter.WKT;
        Integer LineRDDNumPartitions = 1;
        Integer LineRDDStartOffset = 1;
        Integer LineRDDEndOffset = -1;

        LineStringRDD osm1AllWays = new LineStringRDD(sc,LineRDDInputLocation,LineRDDStartOffset,LineRDDEndOffset,LineRDDSplitter,true,1);
        osm1AllWays.saveAsGeoJSON(outputFile);
    }
}
