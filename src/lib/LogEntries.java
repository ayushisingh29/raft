package lib;

public class LogEntries {

    public int index;
    public int term;
    public int command;


    public LogEntries(int index, int term, int command) {
        this.index = index;
        this.term = term;
        this.command = command;
    }

}
