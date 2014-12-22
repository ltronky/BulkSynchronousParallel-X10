package x10.util.bsp;

import x10.util.ArrayList;
import x10.util.Team;
import x10.util.List;

/**
   The execution engine for a Job[S,T].
 */
public class JobRunner[S,T](job:Job[S,T]){T <: Agent[S, T]} {
    val activeAgents = [new ArrayList[T]() as List[T], new ArrayList[T]()];
    public def activate(a:T, phase:Int) { atomic activeAgents((phase+1)%2).add(a);}
    public def hasNoActiveAgents(phase:Int):Boolean = activeAgents(phase%2).isEmpty();
    static val AND = new Reducible[Boolean]() {
        public def zero()=true;
        public operator this(a:Boolean, b:Boolean)=a&&b;
    };
    
    public static def say(s:String, phase:Int){
    	Console.OUT.println("[" + here + "," + phase + "]" + s);
    }
    /**
     * Run the supplied job until globally there are no active agents, or 
     * job.shouldRunAgain(phase) returns false. The initial set of active
     * agents is taken from job.startAgents().
     * 
     */
    public static def submit[S,T](job:Job[S,T]){T<: Agent[S,T]} {
        val jobRunners = PlaceLocalHandle.make[JobRunner[S,T]](Place.places(), ()=>new JobRunner[S, T](job));
        //Init startAgents
        finish for (p in Place.places()) at (p) async {
        	if (jobRunners().job.startAgents() != null)
        		jobRunners().activeAgents(0).addAll(jobRunners().job.startAgents());
        }
        
        var gPhase:Int=0n;
        var noActive:Boolean = false;
        do {
        	val phase = gPhase;
        	finish for (p in Place.places()) at (p) async {
	            val agents = jobRunners().activeAgents(phase%2);
            	if (agents != null) {
            		for (agent in agents)
            			agent.run(phase, jobRunners);
            		agents.clear();
            	}
        	} // async, at, for, finish
        	Console.OUT.println("ExecutedPhase(" + phase + ")");
        	val nextPhase = phase+1n;
            noActive = finish (AND) {
                for (place in Place.places()) 
                    at (place) 
                        async 
                        offer jobRunners().hasNoActiveAgents(nextPhase);
            };

            gPhase++;
        } while (job.shouldRunAgain(gPhase) && !noActive);
    }
}
