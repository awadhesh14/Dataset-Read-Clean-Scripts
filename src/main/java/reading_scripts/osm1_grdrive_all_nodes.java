package reading_scripts;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.spatialRDD.PointRDD;

import java.util.Random;

public class osm1_grdrive_all_nodes {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("GeoSparkRunnableExample").setMaster("local[*]");
        conf.set("spark.serializer", KryoSerializer.class.getName());
        //conf.set("spark.kryo.registrator", GeoSparkVizKryoRegistrator.class.getName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        String resourceFolder = System.getProperty("user.dir")+"/src/main/resources/";

        String PointRDDInputLocation  = resourceFolder + "osm1_gdrive_all_nodes_small.tsv";
        String outputFile = resourceFolder + "osm1_gdrive_all_nodes_small" + new Random().nextInt();

        FileDataSplitter PointRDDSplitter = FileDataSplitter.TAB;
        Integer PointRDDNumPartitions = 1;
        Integer PointRDDStartOffset = 1;
        //Integer PointRDDEndOffset = -1;

        PointRDD osm1_nodes = new PointRDD(sc,PointRDDInputLocation,PointRDDStartOffset,PointRDDSplitter,true,PointRDDNumPartitions);
        osm1_nodes.saveAsGeoJSON(outputFile);
    }
}
