package x10.util.bsp;

import x10.util.ArrayList;
import x10.util.Team;
import x10.util.List;

/**
   The execution engine for a Job[S,T].
 */
public class JobRunner[S,T](job:Job[S,T]){T <: Agent[S, T]} {
    val agents:ArrayList[T] = new ArrayList[T]();
    static val OR = new Reducible[Boolean]() {
    	public def zero()=false;
    	public operator this(a:Boolean, b:Boolean)=a||b;
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
        	jobRunners().agents.addAll(jobRunners().job.startAgents());
        }
        
        var gPhase:Int=0n;
        var isThereAnyActive:Boolean = false;
        do {
        	val phase = gPhase;
        	isThereAnyActive = finish(OR) {
        		for (p in Place.places()) at (p) async {
		            var act:Boolean = false;
            		for (agent in  jobRunners().agents) {
            			act = agent.run(phase) || act;
            		}
	            	offer act;
	        	}
        	};// async, at, for, finish
        	Console.OUT.println("ExecutedPhase(" + phase + ")");
            gPhase++;
        } while (job.shouldRunAgain(gPhase) && isThereAnyActive);
    }
}
