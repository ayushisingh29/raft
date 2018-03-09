import java.util.*;
import java.util.Map.Entry;

public class Globals {
    public static int totalVotesEligible = 0;
    public static HashMap<RaftNode,Integer> candidateVotesMap = new HashMap<>();

    public static void reset() {
        totalVotesEligible = 0;
        candidateVotesMap.clear();
    }

    public static RaftNode getWinner() {
        RaftNode winner = null;
        int maxVotes = 0;
        for(RaftNode k : candidateVotesMap.keySet())  {

            if(candidateVotesMap.get(k) == maxVotes) {
                return null;
            }
            else if(candidateVotesMap.get(k) > maxVotes) {
                maxVotes = candidateVotesMap.get(k);
                winner   = k;
            }
        }


        for (RaftNode name: candidateVotesMap.keySet()){

            int id =name.id;
            String value = candidateVotesMap.get(name).toString();
            //System.out.println(id + " " + value);

        }

        //System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n Drumrolls.....Badum Tsssss!!!!!  Winner " + winner.id +  " with votes " + maxVotes +" for term " + winner.currentTerm +"\n\n\n\n\n\n\n\n\n\n");

        return winner;
    }
}
