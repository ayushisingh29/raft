import lib.*;

import java.io.*;
import java.rmi.RemoteException;

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

        if(this.isFollower) {

        }

        else if(this.isCandidate) {

        }

        int lastLogIndex = 0;
        int lastTerm = 0;

        if(log != null && log.length != 0) {
            lastLogIndex = this.log.length-1;
            lastTerm     =  log[log.length-1].term;
        }

        int dest_id = controller.getNumRegistered();

        while(dest_id >= 0){
            RequestVoteArgs requestVoteArgs = new RequestVoteArgs(this.currentTerm, this.id, lastLogIndex, lastTerm);
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream out = null;

            try {
                out = new ObjectOutputStream(byteArrayOutputStream);
                out.writeObject(requestVoteArgs);
                out.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }

            byte[] byteMessage = byteArrayOutputStream.toByteArray();
            System.out.println("Sending from " + (this.id));
            System.out.println(dest_id);
            Message message = new Message(MessageType.RequestVoteArgs, this.id , dest_id,  byteMessage);

            try {
                lib.sendMessage(message);
            } catch (RemoteException e) {
                e.printStackTrace();
            }
            dest_id--;
        }
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
        return new GetStateReply(this.currentTerm, this.isLeader);
 //       return null;
    }

    @Override
    public Message deliverMessage(Message message) {
        MessageType type   = message.getType();
        byte[] byteMessage = message.getBody();
        int src            = message.getSrc();
        int dest           = message.getDest();
        Object obj         = null;

        ByteArrayInputStream in = new ByteArrayInputStream(byteMessage);
        ObjectInputStream is = null;
        try {
            is = new ObjectInputStream(in);
            obj = is.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        if(type == MessageType.RequestVoteArgs) {
            System.out.println("RequestVoteArgs");
            RequestVoteArgs requestVoteArgs = (RequestVoteArgs) obj;
        } else if(type == MessageType.RequestVoteReply) {
            System.out.println("REquestVoteReply");
            RequestVoteReply requestVoteReply = (RequestVoteReply) obj;
        } else if(type == MessageType.AppendEntriesArgs) {
            System.out.println("AppendEntriesArgs");
            AppendEntriesArgs appendEntriesArgs = (AppendEntriesArgs) obj;
        } else {
            System.out.println("AppendEntriesReply");
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
        System.out.println(" Creating node for id - " + args[1]);
        if(controller == null) {
            controller = new Controller(9000);
        }
        RaftNode raftNode   = new RaftNode(9000, Integer.parseInt(args[0]), Integer.parseInt(args[1]));

    }

}
