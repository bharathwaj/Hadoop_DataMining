package edu.uic.ids561.decision;

import java.util.*;

public class DTreeNode {
	private DTreeNode parent;
	public DTreeNode[] children;
	private ArrayList<DTreeRecord> data;
	private double entropy;
	private boolean isUsed;
	private DiscreteAttribute testAttribute;

	public DTreeNode() {
		this.data = new ArrayList<DTreeRecord>();
		setEntropy(0.0);
		setParent(null);
		setChildren(null);
		setUsed(false);
		setTestAttribute(new DiscreteAttribute("", 0));
	}

	public void setParent(DTreeNode parent) {
		this.parent = parent;
	}

	public DTreeNode getParent() {
		return parent;
	}

	public void setData(ArrayList<DTreeRecord> data) {
		this.data = data;
	}

	public ArrayList<DTreeRecord> getData() {
		return data;
	}

	public void setEntropy(double entropy) {
		this.entropy = entropy;
	}

	public double getEntropy() {
		return entropy;
	}

	public void setChildren(DTreeNode[] children) {
		this.children = children;
	}

	public DTreeNode[] getChildren() {
		return children;
	}

	public void setUsed(boolean isUsed) {
		this.isUsed = isUsed;
	}

	public boolean isUsed() {
		return isUsed;
	}

	public void setTestAttribute(DiscreteAttribute testAttribute) {
		this.testAttribute = testAttribute;
	}

	public DiscreteAttribute getTestAttribute() {
		return testAttribute;
	}
}
