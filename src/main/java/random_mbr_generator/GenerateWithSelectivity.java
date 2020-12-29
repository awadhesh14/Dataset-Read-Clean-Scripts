package random_mbr_generator;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.util.GeometricShapeFactory;

import java.awt.*;
import java.util.Random;

public class GenerateWithSelectivity {

    double X1; double X2; double Y1; double Y2; int selectivity;
    Random random;
    public GenerateWithSelectivity(double x1, double x2, double y1, double y2, int selectivity,Random random) {
        this.X1 = x1;
        this.X2 = x2;
        this.Y1 = y1;
        this.Y2 = y2;
        this.selectivity = selectivity;
        this.random = random;

    }
    public Polygon get_random_rectangle(){
        double Area = (Y2-Y1) * (X2-X1);
        double area = Area/selectivity;
        double max_possible_ylen = (Y2-Y1);
        double min_possible_xlen = area/max_possible_ylen;
        //System.out.println(Area + " " +area);
        double xlen = min_possible_xlen + random.nextDouble() * (X2 - min_possible_xlen);
        double ylen = area / xlen;
        double baserange_y = Y2 - ylen;
        double baserange_x = X2 - xlen;
        double base_x = X1 + random.nextDouble() * (baserange_x - X1);
        double base_y = Y1 + random.nextDouble() * (baserange_y - Y1);
        GeometricShapeFactory gsf = new GeometricShapeFactory();
        //System.out.println(xlen + " "+ ylen + " " + xlen*ylen);
        gsf.setWidth(xlen);
        gsf.setHeight(ylen);
        gsf.setBase(new Coordinate(base_x,base_y));
        //gsf.setRotation(0.5); // cant figure out how to rotate within bounds maintaining selectivity
        Polygon rect = gsf.createRectangle();
        return rect;
        /*System.out.println(rect.getArea());
        System.out.println(rect.getEnvelopeInternal());*/

    }

    public static void main(String[] args) {
        GenerateWithSelectivity g = new GenerateWithSelectivity(0,100,0,100,16,new Random(22));
        for(int i=0;i<5;i++){
            Polygon r = g.get_random_rectangle();
            System.out.println(r.getEnvelopeInternal() + "  \t" + r.getArea());
        }
    }
}
