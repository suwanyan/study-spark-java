package swy.study.spark.core;

import java.io.Serializable;

import scala.math.Ordered;

/*
 * 自定义二次排序
 */
public class SecondarySortKey implements Ordered<SecondarySortKey>, Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2567655290340527997L;
	private int first;
	private int second;
	
	public SecondarySortKey(int first, int second) {
		super();
		this.first = first;
		this.second = second;
	}

	public boolean $greater(SecondarySortKey ssk) { // >
		//
		if (this.first > ssk.getFirst()) { // 第一列大
			return true;
		} else if(this.first == ssk.getFirst() &&  // 第一列等于且第二列大
				this.second > ssk.getSecond()) {
			return true;
		}
		return false;
	}

	public boolean $greater$eq(SecondarySortKey ssk) { // >=
		if(this.$greater(ssk)) {
			return true;
		} else if(this.first == ssk.getFirst() && 
				this.second == ssk.getSecond()) {
			return true;
		}
		return false;
	}

	public boolean $less(SecondarySortKey ssk) {  //<
		if (this.first < ssk.getFirst()) {
			return true;
		} else if(this.first < ssk.getFirst() &&
				this.second < ssk.getSecond()) {
			return true;
		}
		return false;
	}

	public boolean $less$eq(SecondarySortKey ssk) { //<=
		if(this.$less(ssk)) {
			return true;
		} else if(this.first == ssk.getFirst() && 
				this.second == ssk.getSecond()) {
			return true;
		}
		return false;
	}

	public int compare(SecondarySortKey ssk) {
		if(this.first - ssk.getFirst() !=0) {
			return this.first - ssk.getFirst();
		} else {
			return this.second - ssk.getSecond();
		}
	}

	public int compareTo(SecondarySortKey ssk) {
		if(this.first - ssk.getFirst() !=0) {
			return this.first - ssk.getFirst();
		} else {
			return this.second - ssk.getSecond();
		}
	}

	public int getFirst() {
		return first;
	}

	public void setFirst(int first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + first;
		result = prime * result + second;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SecondarySortKey other = (SecondarySortKey) obj;
		if (first != other.first)
			return false;
		if (second != other.second)
			return false;
		return true;
	}
	
}
