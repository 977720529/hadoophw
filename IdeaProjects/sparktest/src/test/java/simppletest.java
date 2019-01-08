import java.util.Random;

public class simppletest {
    private static double getDistance(double longt1, double lat1, double longt2, double lat2){

        final double PI = 3.14159265358979323; //圆周率

        final double R = 6371229;
        double x,y, distance;

        x=(longt2-longt1)*PI*R*Math.cos( ((lat1+lat2)/2)*PI/180)/180;

        y=(lat2-lat1)*PI*R/180;

        distance=Math.hypot(x,y);

        return distance;

    }

    static double getDistance1(double longt1, double lat1, double longt2, double lat2){

        final double PI = 3.14159265358979323; //圆周率
        final double R = 6371229;

        double x1,y1,z1,x2,y2,z2, distance;
        x1 = R * Math.sin(longt1 * PI / 180) * Math.cos(PI/2 - (lat1 * PI / 180));
        y1 = R * Math.sin(longt1 * PI /180) * Math.sin(PI/2 - (lat1 * PI / 180));
        z1 = R * Math.cos(PI - (lat1 * PI / 180));

        x2 = R * Math.sin(longt2 * PI / 180) * Math.cos(PI/2 - (lat2 * PI / 180));
        y2 = R * Math.sin(longt2 * PI /180) * Math.sin(PI/2 - (lat2 * PI / 180));
        z2 = R * Math.cos(PI/2 - (lat1 * PI / 180));

        distance=Math.sqrt((x1-x2)*(x1-x2)+(y1-y2)*(y1-y2)+(z1-z2)*(z1-z2));

        return distance;

    }
    public static void main(String[] args) throws Exception{
        double long1, lat1, long2, lat2;
        double dis1, dis2;
        Random random = new Random();
        long1 = random.nextDouble() % 180;
        long2 = random.nextDouble() % 180;
        lat1 = random.nextDouble() % 90;
        lat2 = random.nextDouble() % 90;
        long1 = 120;
        long2 = 121;
        lat1 = 0;
        lat2 = 0;
        dis1 = getDistance(long1, lat1, long2, lat2);
        dis2 = getDistance1(long1, lat1, long2, lat2);
        System.out.println(dis1 + " " + dis2);
    }
}
