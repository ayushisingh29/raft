import lib.*;

import java.io.*;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

public class RaftNode implements MessageHandling, Runnable {
    private long lastCandidateTime;
    public int id;
    private static TransportLib lib;
    private int num_peers;

    int currentTerm;
    private int lastApplied;
    private int commitIndex;
    private int votedFor;
    private int nextIndex [];
    private int matchIndex [];
    private ArrayList<LogEntries> logEntries;
    private long lastHeartBeat;

    private volatile boolean isCandidate = false;
    private volatile boolean isLeader    = false;
    private volatile boolean isFollower  = false;

    static Controller controller ;
    RemoteController remoteController;

    Logger logger;
    FileHandler fh;

    private GetStateReply getStateReply;
    private int heartBeatTimeout;
    private long electionTimeout;
    private boolean hasVoted;
    private int votesGained;
    private int lastVotedTerm;



    public RaftNode(int port, int id, int num_peers) {
        try {

            int port1 = port;

            this.id          = id;
            this.num_peers   = num_peers;
            this.currentTerm = 0;
            this.lastApplied = 0;
            this.commitIndex = 0;
            this.votedFor    = -1;

            this.nextIndex   = new int[this.num_peers];
            this.matchIndex  = new int[this.num_peers];
            this.isFollower  = true;
            this.hasVoted    = false;

            this.getStateReply   = new GetStateReply(0, false);
            this.heartBeatTimeout = getRandom(500, 900);
            this.votesGained     = 0;
            this.lastHeartBeat   = System.currentTimeMillis();
            this.electionTimeout = this.heartBeatTimeout;
            this.logEntries        = new ArrayList<>();
            this.lastCandidateTime = System.currentTimeMillis();
            this.lastVotedTerm     = 0;

            lib = new TransportLib(port1, id, this);
            Thread leaderElectorThread = new Thread(this);
            leaderElectorThread.start();

        }
        catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    private int getRandom(int min, int max) {

        //note a single Random object is reused here
        Random randomGenerator = new Random();
        int randomInt = (randomGenerator.nextInt(min) + (max - min));
        return randomInt;

    }

    /**
     * Function to set the state of current node
     * @param role
     */
    private void setState(String role) {
        if(role.equals(Constants.Roles.CANDIDATE)) {
            this.isCandidate    = true;
            this.isFollower     = false;
            this.isLeader       = false;
        }
        else if(role.equals(Constants.Roles.FOLLOWER)) {
            this.isFollower     = true;
            this.isCandidate    = false;
            this.isLeader       = false;
        }
        else if(role.equals(Constants.Roles.LEADER)) {
            this.isLeader       = true;
            this.isCandidate    = false;
            this.isFollower     = false;
        }
    }


    /**
     * Thread that kicks off leader election
     */
    @Override
    public void run() {

        while(true) {

            if(this.isLeader) {

                this.setState(Constants.Roles.LEADER);

                this.getStateReply.term = this.currentTerm;
                this.getStateReply.isLeader =  true;

                while(this.isLeader) {

                    int lastLogIndex = -1;
                    int lastLogTerm  = -1;

                    if(this.logEntries.size() != 0) {

                        lastLogIndex = this.logEntries.get(this.logEntries.size()-1).index;
                        lastLogTerm  = this.logEntries.get(this.logEntries.size()-1).term;

                    }

                    AppendEntriesArgs appendEntriesArgs = new AppendEntriesArgs(this.currentTerm, this.id,
                            lastLogIndex, lastLogTerm, null, -1);

                    int max_dest_id = this.num_peers-1;

                    while(max_dest_id >= 0){

                        if(max_dest_id != this.id) {

                            try {

                                lib.sendMessage(getMessageBundled(appendEntriesArgs, this.id, max_dest_id));
                            }

                            catch (RemoteException e) {
                                e.printStackTrace();
                            }
                        }
                        max_dest_id--;
                    }

                    long time = System.currentTimeMillis();

                    while(System.currentTimeMillis() - time < 200) {}

                }

            }

            if(this.isFollower) {
                this.votesGained = 0;
                this.setState(Constants.Roles.FOLLOWER);
                this.getStateReply.term = this.currentTerm;
                this.getStateReply.isLeader = false;

                while(this.isFollower) {

                    if((System.currentTimeMillis() - this.lastHeartBeat) > this.heartBeatTimeout)  {

                        this.setState(Constants.Roles.CANDIDATE);
                        break;

                    }
                }
            }

            while(this.isCandidate) {

                this.setState(Constants.Roles.CANDIDATE);
                this.getStateReply.term = this.currentTerm;
                this.getStateReply.isLeader = false;
                this.votesGained = 0;

                this.currentTerm++;

                this.lastCandidateTime = System.currentTimeMillis();


                Object obj = null;
                ObjectInputStream is = null;

                byte[] byteMessage = null;
                ByteArrayInputStream in = null;


                int lastLogIndex = -1;
                int lastLogTerm  = -1;

                if(this.logEntries.size() != 0) {

                    lastLogIndex = this.logEntries.get(this.logEntries.size()-1).index;
                    lastLogTerm  = this.logEntries.get(this.logEntries.size()-1).term;

                }

                if(this.lastVotedTerm < this.currentTerm) {
                    this.votesGained++;
//                    this.hasVoted = true;
                    this.lastVotedTerm = this.currentTerm;
                }

                RequestVoteArgs requestVoteArgs = new RequestVoteArgs(this.currentTerm, this.id, lastLogIndex, lastLogTerm);

                for(int max_dest_id1 = 0; max_dest_id1 < this.num_peers; max_dest_id1++) {

                    try {

                        Message message = null;

                        if(max_dest_id1 != this.id) {

                            message =  lib.sendMessage(getMessageBundled(requestVoteArgs, this.id, max_dest_id1));

                            if(message != null) {
                                byteMessage = message.getBody();
                                in = new ByteArrayInputStream(byteMessage);
                                is = new ObjectInputStream(in);
                                obj = is.readObject();

                                RequestVoteReply requestVoteReply = (RequestVoteReply) obj;

                                if (requestVoteReply.voteGranted) {
                                    this.votesGained++;
                                }

                                if(this.votesGained  > (this.num_peers)/2) {

                                    this.votesGained = 0;
                                    this.setState(Constants.Roles.LEADER);
                                    //resetVotes();
                                    //break;

                                }
                            }
                        }
                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    @Override
    public Message deliverMessage(Message message) {

        try {
            Object obj = null;
            ObjectInputStream is = null;
            MessageType type = message.getType();
            byte[] byteMessage = null;
            ByteArrayInputStream in = null;


            if(type == MessageType.UpdateCommits) {
                UpdateCommits updateCommits = null;

                byteMessage = message.getBody();
                in = new ByteArrayInputStream(byteMessage);
                is = new ObjectInputStream(in);
                obj = is.readObject();

                updateCommits = (UpdateCommits) obj;

                for( int i = this.commitIndex; i < this.logEntries.size(); i++) {

                    this.commitIndex++;

                    ApplyMsg applyMsg =  new ApplyMsg(this.id, this.commitIndex, updateCommits.logEntries.get(i).command, false, null);

                    try {

                        //System.out.println(" Applying state - \n Id - "  + this.id + "\n Commit Index - " + this.commitIndex
                          //      +"\n Command : " + updateCommits.logEntries.get(i).command);

                        lib.applyChannel(applyMsg);
                        //System.out.println("End of applying state \n");

                    }

                    catch(Exception ex) {
                        ex.printStackTrace();
                    }

                }

                return null;
            }

            if (type == MessageType.RequestVoteArgs) {

                RequestVoteReply requestVoteReply = null;

                byteMessage = message.getBody();
                in = new ByteArrayInputStream(byteMessage);
                is = new ObjectInputStream(in);
                obj = is.readObject();

                RequestVoteArgs requestVoteArgs = (RequestVoteArgs) obj;

                int lastLogIndex = -1;
                int lastLogTerm  = -1;

                if(this.logEntries.size() != 0) {

                    lastLogIndex = this.logEntries.get(this.logEntries.size()-1).index;
                    lastLogTerm  = this.logEntries.get(this.logEntries.size()-1).term;

                }

                if(lastLogTerm < requestVoteArgs.lastLogTerm) {

                    if(this.lastVotedTerm < requestVoteArgs.term) {

                        this.lastVotedTerm = requestVoteArgs.term;
                        this.setState(Constants.Roles.FOLLOWER);
                        this.votesGained = 0;
                        requestVoteReply = new RequestVoteReply(requestVoteArgs.term, true);
                        return getMessageBundled(requestVoteReply, message.getDest(), message.getSrc());

                    }
                }

                else if(lastLogTerm == requestVoteArgs.lastLogTerm) {

                    if(lastLogIndex < requestVoteArgs.lastLogIndex  && ( this.lastVotedTerm < requestVoteArgs.term)) {

                        this.lastVotedTerm = requestVoteArgs.term;
                        this.setState(Constants.Roles.FOLLOWER);
                        this.votesGained = 0;
                        requestVoteReply = new RequestVoteReply(requestVoteArgs.term, true);
                        return getMessageBundled(requestVoteReply, message.getDest(), message.getSrc());

                    }

                    else if(lastLogIndex == requestVoteArgs.lastLogIndex && (this.lastVotedTerm < requestVoteArgs.term)) {
                        if(requestVoteArgs.term > this.currentTerm) {

                            this.lastVotedTerm = requestVoteArgs.term;
                            this.setState(Constants.Roles.FOLLOWER);
                            this.votesGained = 0;
                            requestVoteReply = new RequestVoteReply(requestVoteArgs.term, true);
                            return getMessageBundled(requestVoteReply, message.getDest(), message.getSrc());

                        }
                    }
                }

                else {

                    requestVoteReply = new RequestVoteReply(requestVoteArgs.term, false);
                    return getMessageBundled(requestVoteReply, message.getDest(), message.getSrc());

                }

            } else if (type == MessageType.AppendEntriesArgs) {

                byteMessage = message.getBody();
                in = new ByteArrayInputStream(byteMessage);
                is = new ObjectInputStream(in);
                obj = is.readObject();

                AppendEntriesArgs appendEntriesArgs = (AppendEntriesArgs) obj;

                //If heartbeat
                if (appendEntriesArgs.entries == null) {

                    if (this.currentTerm <= appendEntriesArgs.term) {

                        this.currentTerm = appendEntriesArgs.term;
                        this.setState(Constants.Roles.FOLLOWER);
                        this.getStateReply.term = this.currentTerm;
                        this.getStateReply.isLeader = false;
                        this.lastHeartBeat = System.currentTimeMillis();

                    }
                }

                else {

                    int lastTerm  = appendEntriesArgs.prevLogTerm;
                    int lastIndex = appendEntriesArgs.prevLogIndex;

                    if (this.logEntries.size() != 0) {

                        if(lastTerm < this.logEntries.get(this.logEntries.size() - 1).term) {
                            return null;
                        }

                        else if(lastTerm == this.logEntries.get(this.logEntries.size() - 1).term){

                            if(this.commitIndex > appendEntriesArgs.leaderCommit) {
                                return null;
                            }
                        }

                    }


                    LogEntries lastEntry = null;

                    if(this.logEntries.size() != 0) {
                        lastEntry = this.logEntries.get(this.logEntries.size() - 1);
                    }


                    //if not a commit message
                    LogEntries entry = appendEntriesArgs.entries[appendEntriesArgs.entries.length-1];

//                    System.out.println( " Log entry requested from leader - " + message.getSrc() + " to peer "+ this.id +" for term - " +
  //                          appendEntriesArgs.term + " and command " + appendEntriesArgs.entries[appendEntriesArgs.entries.length-1].command);

    //                System.out.println( " Current log size of peer number " + this.id + " is " +
      //                      this.logEntries.size() + ". Adding entry to it.");

                    if( lastEntry == null) {
                        //System.out.println("No entry present. adding entry");

                        //this.logEntries = new ArrayList<LogEntries>(Arrays.asList(appendEntriesArgs.entries));

                        this.logEntries.add(entry);


                        AppendEntriesReply reply = new AppendEntriesReply(appendEntriesArgs.term,true);
                        printLog(this.logEntries.toArray(new LogEntries[this.logEntries.size()]));
                        //System.out.println("End of No entry present. adding entry");
                        return getMessageBundled(reply, this.id, message.getSrc());

                    }
                    else {

                        if(lastTerm == lastEntry.term && lastIndex == lastEntry.index) {
                            //System.out.println("last index and term matched. adding entry");

                            this.logEntries.add(entry);

                            AppendEntriesReply reply = new AppendEntriesReply(appendEntriesArgs.term,true);
                            printLog(this.logEntries.toArray(new LogEntries[this.logEntries.size()]));
                            //System.out.println("End of last index and term matched. adding entry");
                            return getMessageBundled(reply, this.id, message.getSrc());

                        }
                        else {

                            //System.out.println("Last index and term does not match. adding entry");
                            this.logEntries = new ArrayList<LogEntries>(Arrays.asList(appendEntriesArgs.entries));

                            AppendEntriesReply reply = new AppendEntriesReply(appendEntriesArgs.term,true);

                            printLog(this.logEntries.toArray(new LogEntries[this.logEntries.size()]));
                            //System.out.println("end of Last index and term does not match. adding entry");
                            return getMessageBundled(reply, this.id, message.getSrc());

                        }
                    }

                }

            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Message getMessageBundled(Object o, int src_id, int dest_id) {

        try {
            RequestVoteArgs    requestVoteArgs    = null;
            RequestVoteReply   requestVoteReply   = null;
            AppendEntriesReply appendEntriesReply = null;
            AppendEntriesArgs  appendEntriesArgs  = null;
            UpdateCommits      updateCommits      = null;

            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream out = null;
            out = new ObjectOutputStream(byteArrayOutputStream);

            MessageType msgType = null;

            if(o instanceof RequestVoteArgs) {
                requestVoteArgs = (RequestVoteArgs) o;
                out.writeObject(requestVoteArgs);
                msgType = MessageType.RequestVoteArgs;
            }
            if(o instanceof RequestVoteReply) {
                requestVoteReply = (RequestVoteReply) o;
                out.writeObject(requestVoteReply);
                msgType = MessageType.RequestVoteReply;
            }
            if(o instanceof AppendEntriesArgs) {
                appendEntriesArgs = (AppendEntriesArgs) o;
                out.writeObject(appendEntriesArgs);
                msgType = MessageType.AppendEntriesArgs;
            }
            if(o instanceof AppendEntriesReply) {
                appendEntriesReply = (AppendEntriesReply) o;
                out.writeObject(appendEntriesReply);
                msgType = MessageType.AppendEntriesReply;
            }
            if(o instanceof UpdateCommits) {
                updateCommits = (UpdateCommits) o;
                out.writeObject(updateCommits);
                msgType = MessageType.UpdateCommits;
            }
            out.flush();

            byte[] byteMessage = byteArrayOutputStream.toByteArray();

            Message message = new Message(msgType , src_id , dest_id,  byteMessage);


            return message;

        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }


    /*
     *call back.
     */

    @Override
    public StartReply start(int command) {

        AppendEntriesArgs appendEntriesArgs = null;

        if(this.getStateReply.isLeader) {

            System.out.println( "Start called with command - "+ command + ". Start called for id - " + this.id  + " is leader = " + this.getStateReply.isLeader);

            LogEntries lastLogEntry = null; //to verify last term and index

            if( this.logEntries.size() == 0) {

                lastLogEntry = new LogEntries(-1, -1 , Integer.MIN_VALUE);

            }

            else {

                lastLogEntry = this.logEntries.get(this.logEntries.size() - 1);

            }

            LogEntries logEntry = new LogEntries(this.logEntries.size()+1, this.currentTerm, command);

            this.logEntries.add(logEntry);

            int max_dest_id  = this.num_peers-1;

            ArrayList<Integer> replied = new ArrayList<>();

            int totalReplied = 1;

            while(max_dest_id >= 0){

                if(max_dest_id != this.id) {

                    try {

                        appendEntriesArgs = new AppendEntriesArgs(this.currentTerm,
                                this.id, lastLogEntry.index, lastLogEntry.term,
                                this.logEntries.toArray(new LogEntries[logEntries.size()]), this.commitIndex);

                        Message message = lib.sendMessage(getMessageBundled(appendEntriesArgs, this.id, max_dest_id));

                        if(message != null) {

                            byte[] byteMessage = message.getBody();
                            ByteArrayInputStream in = new ByteArrayInputStream(byteMessage);
                            ObjectInputStream is = new ObjectInputStream(in);
                            Object obj = is.readObject();

                            AppendEntriesReply reply = (AppendEntriesReply) obj;

                            if(reply.success) {
                                replied.add(message.getSrc());
                                totalReplied++;
                            }

                        }

                    }

                    catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }

                }

                max_dest_id--;
            }

            if(totalReplied > (this.num_peers)/2 ) {

                for( int i = this.commitIndex; i < this.logEntries.size(); i++) {

                    this.commitIndex++;

                    ApplyMsg applyMsg =  new ApplyMsg(this.id, this.commitIndex, this.logEntries.get(i).command, false, null);

                    try {

                        //System.out.println(" Applying state - \n Id - "  + this.id + "\n Commit Index - " + this.commitIndex
                        //       +"\n Command : " + this.logEntries.get(i).command);

                        lib.applyChannel(applyMsg);
                        //System.out.println("End of applying state \n");

                    }

                    catch(Exception ex) {
                        ex.printStackTrace();
                    }

                }

                printLog(this.logEntries.toArray(new LogEntries[this.logEntries.size()]));

                for(int dest : replied) {

                    if (dest != this.id) {

                        //applyMsg = new ApplyMsg(dest, this.commitIndex, command, true, null);

                        //lib.applyChannel(applyMsg);

                        try {
                            UpdateCommits updateCommits = new UpdateCommits(this.logEntries);
                            Message m = lib.sendMessage(getMessageBundled(updateCommits, this.id, dest));

                        } catch (RemoteException e) {

                            e.printStackTrace();

                        }

                    }
                }
            }
            else {
            //    this.logEntries.remove(this.logEntries.size()-1);
            }

            return new StartReply(this.logEntries.size(), appendEntriesArgs.term, true);
        }

        return new StartReply(this.logEntries.size(), this.currentTerm, false);
    }


    void printLog(LogEntries entries[]) {

        // System.out.println(" For ID - " + this.id);
        // for(LogEntries entry  : entries) {
        //     System.out.println( " Index : "  + entry.index + "\n" + " Term : " + entry.term + "\n" + " Command : " + entry.command);
        // }
        // System.out.println("-------------------------------------------------------");

    }


    @Override
    public GetStateReply getState() {
        return this.getStateReply;
    }


    //main function

    /**
     * Main function
     * @param args
     * @throws Exception
     */

    public static void main(String args[]) throws Exception {

        if (args.length != 3) throw new Exception("Need 2 args: <port> <id> <num_peers>");

        RaftNode UN = new RaftNode(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));

    }

}
