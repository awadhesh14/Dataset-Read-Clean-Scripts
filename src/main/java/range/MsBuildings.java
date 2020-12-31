package range;

import com.vividsolutions.jts.geom.Envelope;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialOperator.RangeQuery;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import random_mbr_generator.GenerateWithSelectivity;
import reading_scripts.MSBuildings_UCRstar;

import java.util.ArrayList;
import java.util.Random;

public class MsBuildings {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("GeoSparkRunnableExample").setMaster("local[*]");
        conf.set("spark.serializer", KryoSerializer.class.getName());
        //conf.set("spark.kryo.registrator", GeoSparkVizKryoRegistrator.class.getName());
        JavaSparkContext sc = new JavaSparkContext(conf);
        PolygonRDD MSbuilding = MSBuildings_UCRstar.getMSBuilding(sc);
        MSbuilding.analyze();

        //in the names of the function first word is partitioning and second is indexing
        //partitioning_indexing_range()
        ArrayList<Long> ar, ar2, ar3, ar4, ar5;
        int selectivity = 1, numPartitions = 10;
        long seed = 22;
        Envelope boundary = MSbuilding.boundaryEnvelope;
        GenerateWithSelectivity gws = new GenerateWithSelectivity(boundary.getMinX(), boundary.getMaxX(), boundary.getMinY(), boundary.getMaxY(), selectivity, seed);
        ar = rawrdd_indexing_range(sc, MSbuilding, IndexType.RTREE, gws);
        ar2 = partitioning_indexing_range(sc, MSbuilding, GridType.EQUALGRID, IndexType.RTREE, numPartitions, gws);
        ar3 = partitioning_indexing_range(sc, MSbuilding, GridType.RTREE, IndexType.RTREE, numPartitions, gws);
        ar4 = partitioning_indexing_range(sc, MSbuilding, GridType.QUADTREE, IndexType.RTREE, numPartitions, gws);
        ar.forEach(r -> System.out.printf("%d ", r));
        System.out.println();
        ar2.forEach(r -> System.out.printf("%d ", r));
        System.out.println();
        ar3.forEach(r -> System.out.printf("%d ", r));
        System.out.println();
        ar4.forEach(r -> System.out.printf("%d ", r));
    }

    private static ArrayList<Long> rawrdd_indexing_range(JavaSparkContext sc, PolygonRDD mSbuilding, IndexType indextype, GenerateWithSelectivity gws) throws Exception {
        gws.resetseed();
        mSbuilding.buildIndex(indextype, false);
        mSbuilding.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY());
        ArrayList<Long> ar = new ArrayList<>();
        long temp;
        for (int i = 0; i < 10; i++) {
            temp = RangeQuery.SpatialRangeQuery(mSbuilding, gws.get_random_rectangle(), true, true).count();
            ar.add(temp);
            assert temp > -1;
        }
        return ar;
    }

    static ArrayList<Long> partitioning_indexing_range(JavaSparkContext sc, PolygonRDD mSbuilding, GridType gridtype, IndexType indextype, int numPartitions, GenerateWithSelectivity gws) throws Exception {
        gws.resetseed();
        mSbuilding.spatialPartitioning(gridtype, numPartitions);
        mSbuilding.buildIndex(indextype, true);
        mSbuilding.indexedRDD.persist(StorageLevel.MEMORY_ONLY());
        ArrayList<Long> ar = new ArrayList<>();
        long temp;
        for (int i = 0; i < 10; i++) {
            temp = RangeQuery.SpatialRangeQuery(mSbuilding, gws.get_random_rectangle(), true, true).count();
            ar.add(temp);
            assert temp > -1;
        }
        return ar;
    }
}











    /*int selectivity = 1, numPartitions = 10;
        ArrayList<Long> ar = raw_rtree_range(sc, MSbuilding, selectivity, numPartitions);
        ArrayList<Long> ar2 = equigrid_rtree_range(sc, MSbuilding, selectivity, numPartitions);
        ArrayList<Long> ar3 = rtree_rtree_range(sc, MSbuilding, selectivity, numPartitions);
        ArrayList<Long> ar4 = quadtree_rtree_range(sc, MSbuilding, selectivity, numPartitions);
        */
    /*static ArrayList<Long> raw_rtree_range(JavaSparkContext sc, PolygonRDD mSbuilding, int selectivity, int numPartitions) throws Exception {
        Envelope boundary = mSbuilding.boundaryEnvelope;
        GenerateWithSelectivity gws = new GenerateWithSelectivity(boundary.getMinX(), boundary.getMaxX(), boundary.getMinY(), boundary.getMaxY(), selectivity, new Random(22));
        mSbuilding.buildIndex(IndexType.RTREE, false);
        mSbuilding.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY());
        long temp;
        ArrayList<Long> ar = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            temp = RangeQuery.SpatialRangeQuery(mSbuilding, gws.get_random_rectangle(), true, true).count();
            ar.add(temp);
            assert temp > -1;
        }
        return ar;
    }

    private static ArrayList<Long> equigrid_rtree_range(JavaSparkContext sc, PolygonRDD mSbuilding, int selectivity, int numPartitions) throws Exception {
        Envelope boundary = mSbuilding.boundaryEnvelope;
        GenerateWithSelectivity gws = new GenerateWithSelectivity(boundary.getMinX(), boundary.getMaxX(), boundary.getMinY(), boundary.getMaxY(), selectivity, new Random(22));
        mSbuilding.spatialPartitioning(GridType.EQUALGRID, numPartitions);
        mSbuilding.buildIndex(IndexType.RTREE, true);
        mSbuilding.indexedRDD.persist(StorageLevel.MEMORY_ONLY());
        ArrayList<Long> ar = new ArrayList<>();
        long temp;
        for (int i = 0; i < 10; i++) {
            temp = RangeQuery.SpatialRangeQuery(mSbuilding, gws.get_random_rectangle(), true, true).count();
            ar.add(temp);
            assert temp > -1;
        }
        return ar;
    }

    private static ArrayList<Long> quadtree_rtree_range(JavaSparkContext sc, PolygonRDD mSbuilding, int selectivity, int numPartitions) throws Exception {
        Envelope boundary = mSbuilding.boundaryEnvelope;
        GenerateWithSelectivity gws = new GenerateWithSelectivity(boundary.getMinX(), boundary.getMaxX(), boundary.getMinY(), boundary.getMaxY(), selectivity, new Random(22));
        mSbuilding.spatialPartitioning(GridType.QUADTREE, numPartitions);
        mSbuilding.buildIndex(IndexType.RTREE, true);
        mSbuilding.indexedRDD.persist(StorageLevel.MEMORY_ONLY());
        ArrayList<Long> ar = new ArrayList<>();
        long temp;
        for (int i = 0; i < 10; i++) {
            temp = RangeQuery.SpatialRangeQuery(mSbuilding, gws.get_random_rectangle(), true, true).count();
            ar.add(temp);
            assert temp > -1;
        }
        return ar;
    }



    private static ArrayList<Long> rtree_rtree_range(JavaSparkContext sc, PolygonRDD mSbuilding, int selectivity, int numPartitions) throws Exception{
        Envelope boundary = mSbuilding.boundaryEnvelope;
        GenerateWithSelectivity gws = new GenerateWithSelectivity(boundary.getMinX(), boundary.getMaxX(), boundary.getMinY(), boundary.getMaxY(), selectivity, new Random(22));
        mSbuilding.spatialPartitioning(GridType.RTREE, numPartitions);
        mSbuilding.buildIndex(IndexType.RTREE, true);
        mSbuilding.indexedRDD.persist(StorageLevel.MEMORY_ONLY());
        ArrayList<Long> ar = new ArrayList<>();
        long temp;
        for (int i = 0; i < 10; i++) {
            temp = RangeQuery.SpatialRangeQuery(mSbuilding, gws.get_random_rectangle(), true, true).count();
            ar.add(temp);
            assert temp > -1;
        }
        return ar;
    }*/


