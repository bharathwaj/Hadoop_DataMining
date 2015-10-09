package edu.uic.ids561.decision;


public class DTreeRealAttribute extends DTreeAttribute {

	public DTreeRealAttribute() {
		super("", 0);
	}
	
	public DTreeRealAttribute(String name, double value) {
		super(name, value);
	}
	
	public DTreeRealAttribute(String name, String value) {
		super(name, value);
	}

}
