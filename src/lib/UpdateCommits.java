package lib;

import java.io.Serializable;
import java.util.ArrayList;

public class UpdateCommits implements Serializable {

    private static final long serialVersionUID = 1;
    public ArrayList<LogEntries> logEntries;

    public UpdateCommits(ArrayList<LogEntries> logEntries) {
        this.logEntries = logEntries;
    }
}
