import com.sun.corba.se.impl.orbutil.closure.Constant;
import com.sun.org.apache.bcel.internal.*;
import lib.*;
import lib.Constants;

import java.io.*;
import java.rmi.RemoteException;
import java.util.Random;
import java.util.logging.FileHandler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class RaftNode implements MessageHandling, Runnable {
    private int id;
    private static TransportLib lib;
    private int num_peers;

    private int currentTerm;
    private int lastApplied;
    private int commitIndex;
    private int votedFor;
    private int nextIndex [];
    private int matchIndex [];
    private LogEntries log[];
    private long lastHeartBeat;

    private boolean isCandidate = false;
    private boolean isLeader    = false;
    private boolean isFollower  = false;

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

            int port1 = port;

            this.id          = id;
            this.num_peers   = num_peers;
            this.currentTerm = 0;
            this.lastApplied = 0;
            this.commitIndex = 0;
            this.votedFor    = -1;

            this.nextIndex   = new int[20];
            this.matchIndex  = new int[20];
            this.isFollower  = true;
            this.hasVoted    = false;

            this.getStateReply   = new GetStateReply(-1, false);
            this.electionTimeout = getRandom();
            this.votesGained     = 0;

            this.logger = Logger.getLogger("MyLog" + this.id);
            this.logger.setUseParentHandlers(false);

            try {

                //TODO - Remove the logger code
                /*********************************************************
                 * To remove the logger code later
                 */
                fh = new FileHandler(Constants.Roles.LOGPATH + "LogFile" + this.id + ".log");
                this.logger.addHandler(fh);
                SimpleFormatter formatter = new SimpleFormatter();
                fh.setFormatter(formatter);
                /***********************************************************/

            } catch (SecurityException | IOException e) {
                e.printStackTrace();
            }

//            remoteController = new RemoteController(this);
//            controller.register(this.id, remoteController);

            lib = new TransportLib(port1, id, this);
            Thread leaderElectorThread = new Thread(this);
            leaderElectorThread.start();

        }
        catch (Exception ex) {
            this.logger.info(ex.getLocalizedMessage());
        }

    }

    private int getRandom() {

        //note a single Random object is reused here
        Random randomGenerator = new Random();
        int randomInt = (randomGenerator.nextInt(150) + 150);
        return randomInt;

    }





    /**
     * Clear all counters and variables on state changes
     */
    private void clearSlate() {

        this.votesGained = 0;
        this.hasVoted    = false;
        this.votedFor    = -1;

        this.getStateReply.term     = this.currentTerm;
        this.getStateReply.isLeader = this.isLeader;

        this.lastHeartBeat = System.currentTimeMillis();

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

        this.logger.info("id :: "  + this.id + " Registered all peers. Total peers : " + controller.getNumRegistered());
        processFollower();
    }

    private void processFollower(){
        this.logger.info("id :: " + this.id + " term :: " + this.currentTerm + " is Follower");
        this.setState(Constants.Roles.FOLLOWER);
        this.clearSlate();

        this.getStateReply.isLeader = this.isLeader;
        this.getStateReply.term     = this.currentTerm; //TODO set current term

        while(this.isFollower) {
            long heartBeatInterval = System.currentTimeMillis() - this.lastHeartBeat;
//            this.logger.info("id :: " + this.id + " term :: " + this.currentTerm + " Received Heartbeat, interval = " + heartBeatInterval + " and election timeout = " + this.electionTimeout);

            if(heartBeatInterval > this.electionTimeout)  {
                this.logger.info("id :: " + this.id + " term :: " + this.currentTerm + " Election timeout, changing to candidate");
                this.setState(Constants.Roles.CANDIDATE);
                processCandidate();
            }
        }
    }

    private void processCandidate() {
        this.logger.info("id :: " + this.id + " term :: " + this.currentTerm + " is Candidate");
        this.currentTerm++;
//        if(this.currentTerm == 10) {
//            System.exit(0);
//        }
        int lastLogIndex = 0;
        int lastTerm = 0;
        int max_dest_id= -1;

        this.setState(Constants.Roles.CANDIDATE);
        this.clearSlate();

        this.getStateReply.isLeader = this.isLeader;
        this.getStateReply.term     = this.currentTerm;

        if(log != null && log.length != 0) {
            lastLogIndex = this.log.length-1;
            lastTerm     = log[log.length-1].term;
        }

        RequestVoteArgs requestVoteArgs = new RequestVoteArgs(this.currentTerm, this.id, lastLogIndex, lastTerm);
        max_dest_id = controller.getNumRegistered() - 1;
        this.logger.info("id :: " + this.id + " term :: " + this.currentTerm + " Votes requested for term " + this.currentTerm);

        while(max_dest_id >= 0){
            try {
                lib.sendMessage(getMessageBundled(requestVoteArgs, this.id, max_dest_id));
            } catch (RemoteException e) {
                e.printStackTrace();
            }
            max_dest_id--;
        }
    }

    void processLeader() {
        this.logger.info("id :: " + this.id + " term :: " + this.currentTerm + " elected leader for term " + this.currentTerm);
        this.setState(Constants.Roles.LEADER);
        this.getStateReply.isLeader = this.isLeader;
        this.getStateReply.term     = this.currentTerm;

        while(this.isLeader) {
            AppendEntriesArgs appendEntriesArgs = new AppendEntriesArgs(this.currentTerm, this.id, -1, -1, null, -1);
            int max_dest_id = controller.getNumRegistered()-1;
            while(max_dest_id >= 0 && this.isLeader){
                if(max_dest_id != this.id) {
                    try {
                        //this.logger.info("id :: " + this.id + " term :: " + this.currentTerm + " Sending heartbeat");
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
        try {
            Object obj = null;
            ObjectInputStream is = null;
            MessageType type = message.getType();;
            byte[] byteMessage;
            ByteArrayInputStream in;


            if (type == MessageType.RequestVoteArgs) {

                int src;
                int dest;
                boolean voteGranted;

                src = message.getSrc();
                dest = message.getDest();
                voteGranted = false;

                byteMessage = message.getBody();
                in = new ByteArrayInputStream(byteMessage);
                is = new ObjectInputStream(in);
                obj = is.readObject();

                RequestVoteArgs requestVoteArgs = (RequestVoteArgs) obj;

                if(requestVoteArgs.term > this.currentTerm) {
                    this.currentTerm = requestVoteArgs.term;
                    this.setState(Constants.Roles.FOLLOWER);
                    //this.clearSlate();
                }

                if (this.isCandidate) {
                    src = this.id;
                }
                //give vote
                if (!this.hasVoted) {
                    voteGranted = true;
                    this.logger.info("id :: " + this.id + " term :: " + this.currentTerm + " Vote granted to " + src + " for term " + requestVoteArgs.term);
                } else {
                    voteGranted = false;
                }

                RequestVoteReply requestVoteReply = new RequestVoteReply(requestVoteArgs.term, voteGranted);
                this.hasVoted = true;

                try {
                    lib.sendMessage(getMessageBundled(requestVoteReply, this.id, src));
                    if(this.isFollower) {
                        processFollower();
                    }
                } catch (RemoteException e) {
                    e.printStackTrace();
                }


            } else if (type == MessageType.RequestVoteReply) {
                int src;
                int dest;
                boolean voteGranted;

                src = message.getSrc();
                dest = message.getDest();
                voteGranted = false;

                byteMessage = message.getBody();
                in = new ByteArrayInputStream(byteMessage);
                is = new ObjectInputStream(in);
                obj = is.readObject();

                RequestVoteReply requestVoteReply = (RequestVoteReply) obj;

                if (requestVoteReply.term == this.currentTerm && requestVoteReply.voteGranted) {
                    this.votesGained++;
                    this.logger.info("id :: " + this.id + " term :: " + this.currentTerm + " Vote gained from " + src);
                    this.logger.info("id :: " + this.id + " term :: " + this.currentTerm + " Total votes gained = " + this.votesGained);
                    if (this.votesGained > (this.num_peers-1) / 2) {
                        this.logger.info("id :: " + this.id + " term :: " + this.currentTerm + " Majority votes. Becoming leader");
                        processLeader();
                    }
                }
            } else if (type == MessageType.AppendEntriesArgs) {
                int src;
                int dest;
                boolean voteGranted;

                src = message.getSrc();
                dest = message.getDest();
                voteGranted = false;

                byteMessage = message.getBody();
                in = new ByteArrayInputStream(byteMessage);
                is = new ObjectInputStream(in);
                obj = is.readObject();

                AppendEntriesArgs appendEntriesArgs = (AppendEntriesArgs) obj;

                //If heartbeat
                if (appendEntriesArgs.entries == null) {

                    if (this.isCandidate) {
                        if (appendEntriesArgs.term >= this.currentTerm) {
                            this.logger.info("id :: " + this.id +  " term :: " + this.currentTerm + " Heartbeat received by Candidate");
                            this.logger.info("id :: " + this.id + " term :: " + this.currentTerm + " Heartbeat term greater. Becoming Follower");
                            this.currentTerm = appendEntriesArgs.term;
                            processFollower();
                        }
                    }

                    if (this.isFollower) {

                        long receivedTime = System.currentTimeMillis();
                        long heartBeatInterval = receivedTime - this.lastHeartBeat;
                        this.logger.info("id :: " + this.id + " term :: " + this.currentTerm + " Received Heartbeat, interval = " + heartBeatInterval + " and election timeout = " + this.electionTimeout);

                        this.logger.info("id :: " + this.id + " term :: " + this.currentTerm + " Heartbeat received by Follower");
                        this.lastHeartBeat = receivedTime;
                        if(this.currentTerm != appendEntriesArgs.term) {
                            this.currentTerm = appendEntriesArgs.term;
                        }

                    }
                }
            } else {
                AppendEntriesReply appendEntriesReply = (AppendEntriesReply) obj;

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

        //if (args.length != 3) throw new Exception("Need 2 args: <port> <id> <num_peers>");
        if(controller == null) {
            controller = new Controller(9000);
        }
        //RaftNode UN = new RaftNode(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        //this.logger.info(" Creating node for id - " + args[0]);

        RaftNode raftNode   = new RaftNode(9000, Integer.parseInt(args[0]), Integer.parseInt(args[1]));

    }

}
