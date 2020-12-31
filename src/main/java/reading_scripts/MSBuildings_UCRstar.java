package reading_scripts;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.storage.StorageLevel;

//import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator;

import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;

import java.util.Random;

public class MSBuildings_UCRstar {

    public static PolygonRDD getMSBuilding(JavaSparkContext sc){
        String resourceFolder = System.getProperty("user.dir")+"/src/main/resources/";

        String PolygonRDDInputLocation = resourceFolder + "MSBuildings_UCRstar_small.csv";
        String outputFile = resourceFolder + "MSBuildings_saveAsTextFile_" + new Random().nextInt();

        FileDataSplitter PolygonRDDSplitter = FileDataSplitter.WKT;
        Integer PolygonRDDNumPartitions = 1;
        Integer PolygonRDDStartOffset = 0;
        Integer PolygonRDDEndOffset = -1;

        return new PolygonRDD(sc,PolygonRDDInputLocation,PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true,PolygonRDDNumPartitions);

    }
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("GeoSparkRunnableExample").setMaster("local[*]");
        conf.set("spark.serializer", KryoSerializer.class.getName());
        //conf.set("spark.kryo.registrator", GeoSparkVizKryoRegistrator.class.getName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        String resourceFolder = System.getProperty("user.dir")+"/src/main/resources/";

        String PolygonRDDInputLocation = resourceFolder + "MSBuildings_UCRstar_small.csv";
        String outputFile = resourceFolder + "MSBuildings_saveAsTextFile_" + new Random().nextInt();

        FileDataSplitter PolygonRDDSplitter = FileDataSplitter.WKT;
        Integer PolygonRDDNumPartitions = 1;
        Integer PolygonRDDStartOffset = 0;
        Integer PolygonRDDEndOffset = -1;

        PolygonRDD MSbuilding = new PolygonRDD(sc,PolygonRDDInputLocation,PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true,PolygonRDDNumPartitions);
        MSbuilding.saveAsGeoJSON(outputFile);
    }
}
