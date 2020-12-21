package random_mbr_generator;

import com.vividsolutions.jts.geom.Envelope;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;

import java.util.Random;


public class Generate {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("GeoSparkRunnableExample").setMaster("local[*]");
        conf.set("spark.serializer", KryoSerializer.class.getName());
        //conf.set("spark.kryo.registrator", GeoSparkVizKryoRegistrator.class.getName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        String resourceFolder = System.getProperty("user.dir")+"/src/main/resources/";

        String PolygonRDDInputLocation = resourceFolder + "MSBuildings_UCRstar_small.csv";


        FileDataSplitter PolygonRDDSplitter = FileDataSplitter.WKT;
        Integer PolygonRDDNumPartitions = 1;
        Integer PolygonRDDStartOffset = 0;
        Integer PolygonRDDEndOffset = -1;

        PolygonRDD MSbuilding = new PolygonRDD(sc,PolygonRDDInputLocation,PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true,PolygonRDDNumPartitions);
        MSbuilding.analyze();
        System.out.println(MSbuilding.boundaryEnvelope);
        Double X_min = MSbuilding.boundaryEnvelope.getMinX(),
                X_max = MSbuilding.boundaryEnvelope.getMaxX(),
                Y_min = MSbuilding.boundaryEnvelope.getMinY(),
                Y_max = MSbuilding.boundaryEnvelope.getMaxY();
        Double X_len = X_max - X_min;
        Double Y_len = Y_max - Y_min;
        //System.out.format("%f %f %f %f \n",MSbuilding.boundaryEnvelope.getMinX(),MSbuilding.boundaryEnvelope.getMaxX(),MSbuilding.boundaryEnvelope.getMinY(),MSbuilding.boundaryEnvelope.getMaxY());
        Random random = new Random(22);
        /*for(int i=0;i<10;i++) { // bad method as biased towards upper half in both axis
            Double xmin = X_min + random.nextDouble() * (X_max - X_min);
            Double xmax = xmin + random.nextDouble() * (X_max - xmin);
            Double ymin = Y_min + random.nextDouble() * (Y_max - Y_min);
            Double ymax = ymin + random.nextDouble() * (Y_max - ymin);
            System.out.format("%f %f %f %f\n",xmin,xmax,ymin,ymax);
        }*/
        System.out.println();
        for(int i=0;i<10;i++) {
            Double xcentre = X_min + random.nextDouble() * (X_max - X_min);
            Double xmin = X_min + random.nextDouble() * (xcentre - X_min);
            Double xmax = xcentre + random.nextDouble() * (X_max - xcentre);
            Double ycentre = Y_min + random.nextDouble() * (Y_max - Y_min);
            Double ymin = Y_min + random.nextDouble() * (ycentre - Y_min);
            Double ymax = ycentre + random.nextDouble() * (Y_max - ycentre);
            System.out.println(new Envelope(xmin,xmax,ymin,ymax));
        }
    }
}
