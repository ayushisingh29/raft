package lib;

public class AppendEntriesArgs {

    int term;
    int leaderId;
    int prevLogIndex;
    int prevLogTerm;
    LogEntries[] entries;
    int leaderCommit;

    public AppendEntriesArgs(int term, int leaderId, int prevLogIndex, int prevLogTerm, LogEntries[] entries, int leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }
}
