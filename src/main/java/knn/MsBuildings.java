package knn;

import com.vividsolutions.jts.geom.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialOperator.KNNQuery;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator;
import reading_scripts.MSBuildings_UCRstar;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MsBuildings {
    public static void main(String[] args) throws Exception{
        SparkConf conf = new SparkConf().setAppName("GeoSparkRunnableExample").setMaster("local[*]");
        conf.set("spark.serializer", KryoSerializer.class.getName());
        conf.set("spark.kryo.registrator", GeoSparkVizKryoRegistrator.class.getName());
        JavaSparkContext sc = new JavaSparkContext(conf);
        PolygonRDD MSbuilding = MSBuildings_UCRstar.getMSBuilding(sc);
        MSbuilding.analyze();

        //in the names of the function first word is partitioning and second is indexing
        //partitioning_indexing_range()
        ArrayList<Long> ar, ar2, ar3, ar4, ar5;
        int selectivity = 1, numPartitions = 10, k=193; //TODO: make k random
        long kmin = 1, kmax = MSbuilding.approximateTotalCount;
        long seed = 22;

        ar = rawrdd_indexing_knn( MSbuilding, IndexType.RTREE, k, seed);
        ar2 = partitioning_indexing_knn(MSbuilding, GridType.EQUALGRID, IndexType.RTREE, numPartitions,k,seed);
        ar3 = partitioning_indexing_knn(MSbuilding, GridType.RTREE, IndexType.RTREE, numPartitions, k,seed);
        ar4 = partitioning_indexing_knn(MSbuilding, GridType.QUADTREE, IndexType.RTREE, numPartitions, k , seed);
        ar.forEach(r -> System.out.printf("%d ", r));
        System.out.println();
        ar2.forEach(r -> System.out.printf("%d ", r));
        System.out.println();
        ar3.forEach(r -> System.out.printf("%d ", r));
        System.out.println();
        ar4.forEach(r -> System.out.printf("%d ", r));
    }

    private static ArrayList<Long> rawrdd_indexing_knn(PolygonRDD mSbuilding, IndexType indextype, int k, long seed) throws Exception{
        srandom.setSeed(seed);
        mSbuilding.buildIndex(indextype, false);
        mSbuilding.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY());
        ArrayList<Long> ar = new ArrayList<>();
        List<Polygon>  temp;
        for (int i = 0; i < 10; i++) {
            temp = KNNQuery.SpatialKnnQuery(mSbuilding, getRandomQueryPoint(mSbuilding.boundaryEnvelope), k, true);
            ar.add((long) temp.size());
            assert temp.size() > -1;
        }
        return ar;
    }

    private static ArrayList<Long> partitioning_indexing_knn(PolygonRDD mSbuilding, GridType gridtype, IndexType indextype, int numPartitions, int k, long seed) throws Exception {
        srandom.setSeed(seed);
        mSbuilding.spatialPartitioning(gridtype,numPartitions);
        mSbuilding.buildIndex(indextype, true);
        mSbuilding.indexedRDD.persist(StorageLevel.MEMORY_ONLY());
        ArrayList<Long> ar = new ArrayList<>();
        List<Polygon>  temp;
        for (int i = 0; i < 10; i++) {
            temp = KNNQuery.SpatialKnnQuery(mSbuilding, getRandomQueryPoint(mSbuilding.boundaryEnvelope), k, true);
            ar.add((long) temp.size());
            assert temp.size() > -1;
        }
        return ar;
    }

    static Random srandom = new Random();
    static Point getRandomQueryPoint(Envelope boundary){
        double x1= boundary.getMinX(), x2= boundary.getMaxX(), y1= boundary.getMinY(), y2 = boundary.getMaxY();
        double x = x1 + srandom.nextDouble() * (x2-x1);
        double y = y1 + srandom.nextDouble() * (y2-y1);
        return new GeometryFactory().createPoint(new Coordinate(-84.01, 34.01));
    }
}
