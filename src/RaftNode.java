import lib.*;

import java.io.*;
import java.rmi.RemoteException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class RaftNode implements MessageHandling, Runnable {
    private int id;
    private int port;
    private static TransportLib lib;
    private int num_peers;

    int currentTerm;
    int lastApplied;
    int commitIndex;
    int votedFor;
    int nextIndex [];
    int matchIndex [];
    LogEntries log[];

    boolean isCandidate = false;
    boolean isLeader    = false;
    boolean isFollower  = false;

    static Controller controller ;
    RemoteController remoteController;

    Logger logger;
    FileHandler fh;

    GetStateReply getStateReply;
    int electionTimeout;
    boolean hasVoted;
    int votesGained;


    public RaftNode(int port, int id, int num_peers) {
        try {

            this.id = id;
            this.num_peers = num_peers;
            this.currentTerm = 0;
            this.lastApplied = 0;
            this.commitIndex = 0;
            this.votedFor    = -1;
            this.port        = port;
            this.nextIndex   = new int[20];
            this.matchIndex  = new int[20];
            this.isFollower  = true;
            this.hasVoted    = false;

            this.getStateReply   = new GetStateReply(-1, false);
            this.electionTimeout = (int)((Math.random())% 151) + 150;
            this.votesGained     = 0;

            this.logger = Logger.getLogger("MyLog" + this.id);
            this.logger.setUseParentHandlers(false);
            try {
                // This block configure the logger with handler and formatter
                fh = new FileHandler("C:/Users/ayush/Documents/Sem 4- Spring 2018/Distributed Systems/raft/LogFile" + this.id + ".log");
                this.logger.addHandler(fh);
                SimpleFormatter formatter = new SimpleFormatter();
                fh.setFormatter(formatter);
            } catch (SecurityException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

            remoteController = new RemoteController(this);
            controller.register(this.id, remoteController);

            lib = new TransportLib(port, id, this);
            Thread leaderElectorThread = new Thread(this);
            leaderElectorThread.start();

        }
        catch (Exception ex) {

        }

    }

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

        int lastLogIndex = 0;
        int lastTerm = 0;
        int max_dest_id= -1;

        if(log != null && log.length != 0) {
            lastLogIndex = this.log.length-1;
            lastTerm     = log[log.length-1].term;
        }


        this.logger.info("Run for id :: " + this.id);

        while(this.num_peers != controller.getNumRegistered()){
            //this.logger.info("Waiting for all to get register");
        }

        if(this.isFollower) {

        }

        else if(this.isCandidate) {

            RequestVoteArgs requestVoteArgs = new RequestVoteArgs(this.currentTerm, this.id, lastLogIndex, lastTerm);
            max_dest_id = controller.getNumRegistered()-1;
            while(max_dest_id >= 0){
                if(max_dest_id != this.id) {
                    try {
                        lib.sendMessage(getMessageBundled(requestVoteArgs,this.id, max_dest_id));
                    } catch (RemoteException e) {
                        e.printStackTrace();
                    }
                }
                max_dest_id--;
            }
        }
        else {
            while(this.isLeader) {
                AppendEntriesArgs appendEntriesArgs = new AppendEntriesArgs(this.currentTerm, this.id, -1, -1, null, -1);
                max_dest_id = controller.getNumRegistered()-1;
                while(max_dest_id >= 0 && this.isLeader){
                    if(max_dest_id != this.id) {
                        try {
                            lib.sendMessage(getMessageBundled(appendEntriesArgs,this.id, max_dest_id));
                        } catch (RemoteException e) {
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
        return null;
    }

    @Override
    public GetStateReply getState() {

        this.getStateReply.isLeader = this.isLeader;
        this.getStateReply.term     = this.currentTerm;

        return this.getStateReply;

    }

    @Override
    public Message deliverMessage(Message message) {

        MessageType type   = message.getType();
        byte[] byteMessage = message.getBody();
        int src            = message.getSrc();
        int dest           = message.getDest();
        Object obj         = null;
        this.logger.info("id :: "+ this.id+ " : Received from " + src + " to " + dest);
        ByteArrayInputStream in = new ByteArrayInputStream(byteMessage);
        ObjectInputStream is = null;
        try {
            is = new ObjectInputStream(in);
            obj = is.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        if(type == MessageType.RequestVoteArgs) {
            this.logger.info("RequestVoteArgs");
            RequestVoteArgs requestVoteArgs = (RequestVoteArgs) obj;
        } else if(type == MessageType.RequestVoteReply) {
            this.logger.info("REquestVoteReply");
            RequestVoteReply requestVoteReply = (RequestVoteReply) obj;
        } else if(type == MessageType.AppendEntriesArgs) {
            this.logger.info("AppendEntriesArgs");
            AppendEntriesArgs appendEntriesArgs = (AppendEntriesArgs) obj;
        } else {
            this.logger.info("AppendEntriesReply");
            AppendEntriesReply appendEntriesReply = (AppendEntriesReply) obj;
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
        //if (args.length != 3) throw new Exception("Need 2 args: <port> <id> <num_peers>");
        //new usernode
        //RaftNode UN = new RaftNode(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        //this.logger.info(" Creating node for id - " + args[0]);
        if(controller == null) {
            controller = new Controller(9000);
        }
        RaftNode raftNode   = new RaftNode(9000, Integer.parseInt(args[0]), Integer.parseInt(args[1]));

    }

}
