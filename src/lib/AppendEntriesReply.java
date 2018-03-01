package lib;

public class AppendEntriesReply {

    int term;
    boolean success;

    public AppendEntriesReply(int term, boolean success) {
        this.term = term;
        this.success = success;
    }
}
