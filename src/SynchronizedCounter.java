
public class SynchronizedCounter {
    private int c = 0;
    private long k = 0;
    public synchronized void increment() {
        c++;
    }

    public synchronized void decrement() {
        c--;
    }

    public synchronized int value() {
        return c;
    }
    
    public synchronized int set_value(int value) {
    	c = value; 
        return c;
    }

    public synchronized long set_value(long value) {
    	k = value; 
        return c;
    }

    public synchronized long long_value() {
        return k;
    }
    
}
