package x10.util.bsp;

/**
   A global reference to an Agent[S,T].
 */
public class AgentRef[S,T](ref:GlobalRef[T]){T <: Agent[S,T]} {
    /**
       Accept a message and deliver it to the referenced agent.
     */
    public def accept(m:S, phase:Int){
        ref.evalAtHome((r:T)=>r.accept(m, phase));
    }
    public def toString() = ref.evalAtHome((r:T) => r.toString());
    public def hashCode() = ref.hashCode();
    public def equals(a:Any):Boolean
        = a instanceof AgentRef[S,T] && (a as AgentRef[S,T]).ref==ref;
}