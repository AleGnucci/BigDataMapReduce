package helpers;

/**
 * Interface for classes containing two long values
 * */
public interface CompositeLong {

    long getFirstValue();

    long getSecondValue();

    int compareTo(CompositeLong compositeLong);
}
