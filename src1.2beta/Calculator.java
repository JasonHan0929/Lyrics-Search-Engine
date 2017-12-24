import java.io.File;

public class Calculator {
    public static String timeCalculator (long difference) {
        int hour = (int)(difference / (1000 * 3600));
        difference -= hour * 1000 * 3600;
        int minute = (int)(difference / (1000 * 60));
        difference -= minute * 1000 * 60;
        int second = (int)(difference / 1000);
        return String.format("Time Used: %dh %dm %ds", hour, minute, second);
    }

    public static String sizeCalculator (String... files) {
        long size = 0;
        for (String file : files) {
            size += new File("./result/" + file).length();
        }
        return String.format("Total Size: %.2fMB", size / (double)(1024 * 1024));
    }
}
