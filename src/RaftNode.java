import jdk.nashorn.internal.objects.Global;
import lib.*;

import javax.xml.bind.annotation.XmlElementDecl;
import java.io.*;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

public class RaftNode implements MessageHandling, Runnable {
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
    private int electionTimeout;
    private long heartBeatInterval;
    private boolean hasVoted;
    private int votesGained;



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

            this.getStateReply   = new GetStateReply(-1, false);
            this.electionTimeout = getRandom(500, 900);
            this.votesGained     = 0;
            this.lastHeartBeat   = System.currentTimeMillis();
            this.heartBeatInterval = 0;
            this.logEntries        = new ArrayList<>();

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

                System.out.println(" \n\n Leader - " + this.id + " for term " + this.currentTerm);

                while(this.isLeader) {

                    this.resetVotes();
                    this.getStateReply.term = this.currentTerm;
                    this.getStateReply.isLeader =  true;

                    this.setState(Constants.Roles.LEADER);


                    AppendEntriesArgs appendEntriesArgs = new AppendEntriesArgs(this.currentTerm, this.id, -1, -1, null, -1);

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

                    try {
                        Thread.sleep(110);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                }

            }


            if(this.isFollower) {

                while(this.isFollower) {
                    this.getStateReply.term = this.currentTerm;
                    this.getStateReply.isLeader = false;

                    this.votesGained = 0;

                    this.setState(Constants.Roles.FOLLOWER);

                    if((System.currentTimeMillis() - this.lastHeartBeat) > this.electionTimeout)  {

                        this.setState(Constants.Roles.CANDIDATE);
                        break;

                    }
                }
            }

            if(this.isCandidate) {

                this.votesGained = 0;

                this.getStateReply.term = this.currentTerm;
                this.getStateReply.isLeader = false;
                this.currentTerm++;

                this.setState(Constants.Roles.CANDIDATE);

                Object obj = null;
                ObjectInputStream is = null;

                byte[] byteMessage = null;
                ByteArrayInputStream in = null;

                List<Integer> alives = new ArrayList<>();


                RequestVoteArgs requestVoteArgs = new RequestVoteArgs(this.currentTerm, this.id, 0, 0);


                for(int i = 0 ; i < this.num_peers; i++) {

                    try {
                        Message message = lib.sendMessage(new Message(MessageType.CheckAlive, this.id, i, null));
                        if(message != null && message.getType() == MessageType.CheckAlive) {
                            alives.add(i);
                        }
                    } catch (RemoteException e) {
                        e.printStackTrace();
                    }

                }
                for(int max_dest_id1 = 0; max_dest_id1 < this.num_peers; max_dest_id1++) {//: alives) {

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

                                if(this.votesGained > this.num_peers/2 -1 ) {
                                    this.setState(Constants.Roles.LEADER);
                                    resetVotes();
                                    break;
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

    private void resetVotes() {

        for(int i = 0; i < this.num_peers; i++) {
            try {
                Message m = lib.sendMessage(new Message(MessageType.InvalidateVote, this.id, i, null));
            } catch (RemoteException e) {
                e.printStackTrace();
            }
        }
    }

    private Message getMessageBundled(Object o, int src_id, int dest_id) {
        try {
            RequestVoteArgs    requestVoteArgs    = null;
            RequestVoteReply   requestVoteReply   = null;
            AppendEntriesReply appendEntriesReply = null;
            AppendEntriesArgs  appendEntriesArgs  = null;

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


        long time = System.currentTimeMillis();

        while(System.currentTimeMillis() - time < 2000) {}

        System.out.println( " Start called for id - " + this.id  + " is leader = " + this.isLeader);

        if(this.isLeader) {

            LogEntries lastLogEntry = null;

            if( this.logEntries.size() == 0) {

                lastLogEntry = new LogEntries(-1, this.currentTerm, Integer.MIN_VALUE);

            }

            else {

                lastLogEntry = this.logEntries.get(this.logEntries.size() - 1);

            }

            LogEntries logEntry = new LogEntries(this.logEntries.size(), this.currentTerm, command);

            this.logEntries.add(logEntry);

            int max_dest_id  = this.num_peers-1;

            ArrayList<Integer> replied = new ArrayList<>();

            int totalCommits = 0;

            while(max_dest_id >= 0){

                if(max_dest_id != this.id) {

                    try {

                        AppendEntriesArgs appendEntriesArgs = new AppendEntriesArgs(this.currentTerm, this.id, lastLogEntry.index, lastLogEntry.term, this.logEntries.toArray(new LogEntries[logEntries.size()]), 0);

                        Message message = lib.sendMessage(getMessageBundled(appendEntriesArgs, this.id, max_dest_id));

                        if(message != null) {
                            byte[] byteMessage = message.getBody();
                            ByteArrayInputStream in = new ByteArrayInputStream(byteMessage);
                            ObjectInputStream is = new ObjectInputStream(in);
                            Object obj = is.readObject();

                            AppendEntriesReply reply = (AppendEntriesReply) obj;
                            if(reply.success) {
                                replied.add(max_dest_id);
                                totalCommits++;
                            }
                        }

                    }

                    catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }

                }

                max_dest_id--;
            }

            if(totalCommits > (this.num_peers -1) / 2) {

                this.commitIndex++;

                ApplyMsg applyMsg =  new ApplyMsg(this.id, this.commitIndex,command, false, null);

                try {

                    lib.applyChannel(applyMsg);

                }
                catch (RemoteException e) {

                    e.printStackTrace();

                }


                for(int dest : replied){

                    max_dest_id = dest;

                    if(max_dest_id != this.id) {

                        try {

                            AppendEntriesArgs appendEntriesArgs = new AppendEntriesArgs(this.currentTerm, this.id, lastLogEntry.index, lastLogEntry.term, this.logEntries.toArray(new LogEntries[logEntries.size()]), 1);

                            Message message = lib.sendMessage(getMessageBundled(appendEntriesArgs, this.id, max_dest_id));

                        }

                        catch (IOException e) {

                            e.printStackTrace();

                        }

                    }
                }
            }

            return new StartReply(this.commitIndex, this.currentTerm, this.isLeader);

        }

        return new StartReply(this.commitIndex, this.currentTerm, false);
    }

    @Override
    public GetStateReply getState() {
        return this.getStateReply;
    }

    @Override
    public Message deliverMessage(Message message) {

        try {
            Object obj = null;
            ObjectInputStream is = null;
            MessageType type = message.getType();
            byte[] byteMessage = null;
            ByteArrayInputStream in = null;


            if (type == MessageType.CheckAlive) {
                return new Message(MessageType.CheckAlive, this.id, message.getSrc(), null);
            }

            if (type == MessageType.InvalidateVote) {
                this.hasVoted = false;
                return null;
            }

            if (type == MessageType.RequestVoteArgs) {

                RequestVoteReply requestVoteReply = null;

                byteMessage = message.getBody();
                in = new ByteArrayInputStream(byteMessage);
                is = new ObjectInputStream(in);
                obj = is.readObject();

                RequestVoteArgs requestVoteArgs = (RequestVoteArgs) obj;

                if(requestVoteArgs.term > this.currentTerm) {

                    this.currentTerm = requestVoteArgs.term;
                    this.setState(Constants.Roles.FOLLOWER);

                    this.getStateReply.term = this.currentTerm;
                    this.getStateReply.isLeader = false;

                    if(!this.hasVoted) {

                        this.hasVoted = true;
                        requestVoteReply = new RequestVoteReply(requestVoteArgs.term, true);
                        return getMessageBundled(requestVoteReply, message.getDest(), message.getSrc());

                    }
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

                    if (this.isCandidate) {
                        this.currentTerm = appendEntriesArgs.term;
                        this.setState(Constants.Roles.FOLLOWER);
                        this.lastHeartBeat = System.currentTimeMillis();
                    }

                }

                else {

                    if(appendEntriesArgs.leaderCommit == 0) {

                        //if not a commit message
                        LogEntries entry = appendEntriesArgs.entries[appendEntriesArgs.entries.length-1];

                        System.out.println( " Log entry requested from leader - " + message.getSrc() + " to peer "+ this.id +" for term - " +
                                appendEntriesArgs.term + " and command " + appendEntriesArgs.entries[appendEntriesArgs.entries.length-1].command);

                        System.out.println( " Current log size of peer number " + this.id + " is " +
                                this.logEntries.size() + ". Adding entry to it.");

                        this.logEntries.add(entry);

                        System.out.println( " Current log size of peer number " + this.id + " is " + this.logEntries.size() + ". After adding entry.");

                        AppendEntriesReply reply = new AppendEntriesReply(this.currentTerm,true);
                        return getMessageBundled(reply, this.id, message.getSrc());

                    }

                    else {

                        System.out.println( " Commit requested from leader - " + message.getSrc() + " for term - " +
                                appendEntriesArgs.term + " and command " + appendEntriesArgs.entries[appendEntriesArgs.entries.length-1].command);

                        this.commitIndex++;

                        ApplyMsg applyMsg =  new ApplyMsg(this.id, this.commitIndex,appendEntriesArgs.entries[appendEntriesArgs.entries.length-1].command, false, null);

                        try {

                            lib.applyChannel(applyMsg);
                            return null;

                        }
                        catch (RemoteException e) {

                            e.printStackTrace();

                        }

                    }
                }

            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    //main function

    /**
     * Main function
     * @param args
     * @throws Exception
     */

    public static void main(String args[]) throws Exception {

        if (args.length != 3) throw new Exception("Need 2 args: <port> <id> <num_peers>");
//        if(controller == null) {
//            controller = new Controller(9000);
//        }
        RaftNode UN = new RaftNode(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        //////System.out*.println(" Creating node for id - " + args[0]);

        //RaftNode raftNode   = new RaftNode(9000, Integer.parseInt(args[0]), Integer.parseInt(args[1]));

    }

}
