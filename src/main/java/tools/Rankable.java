package tools;

public interface Rankable extends Comparable<Rankable> {

  Object getObject();

  long getCount();

}
