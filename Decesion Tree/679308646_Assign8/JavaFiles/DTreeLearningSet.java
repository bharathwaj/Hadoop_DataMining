package edu.uic.ids561.decision;

import java.util.*;

public class DTreeLearningSet {
	private ArrayList<DTreeAttributeSet> attributes;

	public DTreeLearningSet() {
		attributes = new ArrayList<DTreeAttributeSet>();
	}

	public void setAttributes(ArrayList<DTreeAttributeSet> attributes) {
		this.attributes = attributes;
	}

	public ArrayList<DTreeAttributeSet> getAttributes() {
		return attributes;
	}
}
