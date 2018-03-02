
public class RunnerClass {
    public static void main(String[] args) {
        int i = 0;

        String arg[] = new String[3];

        while(i < 4) {
            try {

                arg[0] = String.valueOf(9000+i);
                arg[1] = String.valueOf(i);
                arg[2] = "3";
                RaftNode.main(arg);

            } catch (Exception e) {

                e.printStackTrace();

            }
        }
    }
}
