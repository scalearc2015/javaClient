public class Mutex {
	   public void acquire() throws InterruptedException { }
	   public void release() { }
	   public boolean attempt(long msec) throws InterruptedException {
		return false; }
	}
