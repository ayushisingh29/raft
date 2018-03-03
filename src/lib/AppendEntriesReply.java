package lib;

public class AppendEntriesReply {

    private static final long serialVersionUID = 1;
    int term;
    boolean success;

    public AppendEntriesReply(int term, boolean success) {
        this.term = term;
        this.success = success;
    }
}
