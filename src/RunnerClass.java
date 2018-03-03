
public class RunnerClass {
    public static void main(String[] args) {
        int i = 0;

        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS.%1$tL %4$-7s [%3$s] %5$s %6$s%n");

        String arg[] = new String[3];

        while(i < 4) {
            try {
                arg[0] = String.valueOf(i);
                arg[1] = "4";
                RaftNode.main(arg);

            } catch (Exception e) {

                e.printStackTrace();

            }
            i++;
        }
    }
}
