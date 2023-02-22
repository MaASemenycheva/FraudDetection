package ru.ilk.utils;

public class Utils {
    public static Double getDistance(Double lat1, Double lon1, Double lat2, Double lon2)  {
        Integer r = 6371; //Earth radius
        Double latDistance = Math.toRadians(lat2 - lat1);
        Double lonDistance = Math.toRadians(lon2 - lon1);
        Double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        Double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        Double distance = r * c;
        return distance;
    }
}
