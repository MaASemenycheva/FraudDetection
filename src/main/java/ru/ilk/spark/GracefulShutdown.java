package ru.ilk.spark;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.StreamingContext;

public class GracefulShutdown {
    private static Logger logger = Logger.getLogger(GracefulShutdown.class.getName());

    static Boolean stopFlag = false;

    public static void checkShutdownMarker () {
        if (!stopFlag)
        {
            stopFlag =  new java.io.File(SparkConfig.shutdownMarker).exists();
        }
    }

    /* Handle Dstream graceful shutdown. */
    public static void  handleGracefulShutdown (Integer checkIntervalMillis, StreamingContext ssc, SparkSession sparkSession) {
        Boolean isStopped = false;
        while (! isStopped) {
            logger.info("calling awaitTerminationOrTimeout");
            isStopped = ssc.awaitTerminationOrTimeout(checkIntervalMillis);
            if (isStopped)
                logger.info("confirmed! The streaming context is stopped. Exiting application...");
            else
                logger.info("Streaming App is still running. Timeout...");
            checkShutdownMarker();
            if (!isStopped && stopFlag) {
                logger.info("stopping ssc right now");
                ssc.stop(true, true);
                sparkSession.stop();
                logger.info("ssc is stopped!!!!!!!");
            }
        }
    }

}
