
public class LockFreeVector_Testing {
	
	/*
	 * This class is all very disorganized and gross. Please ignore.
	 */

	static final int NUM_THREADS = 8, NUM_OPS = 500, OPS_PER_THREAD = NUM_OPS / NUM_THREADS;
	// Push: [0, PUSH_THRESHOLD), pop: [PUSH_THRESHOLD, 1)
	static final double PUSH_THRESHOLD = 0.99;
	public static void main(String[] args) throws InterruptedException {
		LockFreeVectorWithCombining<Integer> stack = new LockFreeVectorWithCombining<>();
		long startTime = System.currentTimeMillis(), endTime = 0;

		Thread[] threads = new Thread[NUM_THREADS];
		for (int j = 0; j < NUM_THREADS; j++) {
			final int jCopy = j;
			threads[j] = new Thread(() -> {
				useTheStack(stack, jCopy);
			});
			threads[j].start();
		}
		for (int j = 0; j < NUM_THREADS; j++) threads[j].join();
		
		endTime = System.currentTimeMillis();
		System.out.println(NUM_THREADS + ": Time elapsed: " + (endTime - startTime) + " ms");
		System.out.println("NUM_THREADS: " + NUM_THREADS + ", NUM_OPS: " + NUM_OPS + 
				", OPS_PER_THREAD: " + OPS_PER_THREAD);
		System.out.println("Size: " + stack.size());
	}
	
	static void useTheStack(LockFreeVectorWithCombining<Integer> stack, int threadNum) {
		for (int i = 0; i < OPS_PER_THREAD; i++) {
			double op = Math.random();
			if (op < PUSH_THRESHOLD) {
				int n = (int)(Math.random() * 10000);
				stack.pushBack(n);
				System.out.println("Thread " + Thread.currentThread().getId() + " pushed " + n);
			} else {
				Integer n = stack.popBack();
				System.out.println("Thread " + threadNum + " popped " + n);
			}
		}
	}
	
}
