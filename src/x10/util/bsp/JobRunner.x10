package x10.util.bsp;

import x10.util.ArrayList;
import x10.util.Team;
import x10.util.List;

/**
   The execution engine for a Job[S,T].
 */
public class JobRunner[S,T](job:Job[S,T]){T <: Agent[S, T]} {
    val agents:ArrayList[T] = new ArrayList[T]();

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
        	isThereAnyActive = finish(Reducible.OrReducer()) {
        		for (p in Place.places()) at (p) async {
        			var act:Boolean = false;
        			for (agent in  jobRunners().agents) {
        				act = agent.run(phase) || act;
        			}
        			
        			// val act = finish(Reducible.OrReducer()) {
        			// 	for (agent in  jobRunners().agents) async{
        			// 		offer agent.run(phase);
        			// 	}
        			// };
        			offer act;
        		}
        	};// async, at, for, finish
        	Console.OUT.println("ExecutedPhase(" + phase + ")");
            gPhase++;
        } while (job.shouldRunAgain(gPhase) && isThereAnyActive);
    }
}
