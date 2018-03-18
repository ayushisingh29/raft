package lib;

import java.io.Serializable;

public class LogEntries implements Serializable{

    public int index;
    public int term;
    public int command;
    private static final long serialVersionUID = 1;


    public LogEntries(int index, int term, int command) {
        this.index = index;
        this.term = term;
        this.command = command;
    }

}
