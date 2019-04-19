import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class LockFreeVector_Testing {
	
	/*
	 * This class is all very disorganized and gross. Please ignore.
	 */

	static final int NUM_THREADS = 8, NUM_OPS = 100, OPS_PER_THREAD = NUM_OPS / NUM_THREADS;
	// Push: [0, PUSH_THRESHOLD), pop: [PUSH_THRESHOLD, 1)
	static final double PUSH_THRESHOLD = 0.5;
	public static void main(String[] args) throws InterruptedException {
		LockFreeVectorWithCombining<Integer> vector = new LockFreeVectorWithCombining<>();
		long startTime = System.currentTimeMillis(), endTime = 0;
		ConcurrentLinkedQueue<Integer> pushQueue = new ConcurrentLinkedQueue<Integer>(), 
				popQueue = new ConcurrentLinkedQueue<>();

		Thread[] threads = new Thread[NUM_THREADS];
		for (int j = 0; j < NUM_THREADS; j++) {
			final int jCopy = j;
			threads[j] = new Thread(() -> {
				useTheStack(vector, jCopy, pushQueue, popQueue);
			});
			threads[j].start();
		}
		for (int j = 0; j < NUM_THREADS; j++) threads[j].join();
		
		System.out.println("done, calling startCombine()");
//		vector.startCombine(); // Clear out the combining queue
		while (vector.size() > 0) {
			Integer n = vector.popBack();
			if (n != null) popQueue.add(n);
			System.out.println("popped " + n);
		}
		
		
		endTime = System.currentTimeMillis();
		System.out.println(NUM_THREADS + ": Time elapsed: " + (endTime - startTime) + " ms");
		System.out.println("NUM_THREADS: " + NUM_THREADS + ", NUM_OPS: " + NUM_OPS + 
				", OPS_PER_THREAD: " + OPS_PER_THREAD);
		System.out.println("Size: " + vector.size());
		
		// Check the contents of pushQueue and popQueue and make sure they're the same
		TreeMap<Integer, Integer> pushes = new TreeMap<>(), pops = new TreeMap<>();
		System.out.println(pushQueue.size() + " "+ popQueue.size());
		while (!pushQueue.isEmpty()) {
			int n = pushQueue.poll();
			if (pushes.containsKey(n)) pushes.put(n, pushes.get(n) + 1);
			else pushes.put(n, 1);
		}
		while (!popQueue.isEmpty()) {
			int n = popQueue.poll();
			if (pops.containsKey(n)) pops.put(n, pops.get(n) + 1);
			else pops.put(n, 1);
		}
		
		for (Entry<Integer, Integer> e : pushes.entrySet()) {
			System.out.println("Push: " + e + ", pop: " + pops.get(e.getKey()));
		}
	}
	
	static void useTheStack(LockFreeVectorWithCombining<Integer> stack, int threadNum, 
			ConcurrentLinkedQueue<Integer> pushQueue, ConcurrentLinkedQueue<Integer> popQueue) {
		for (int i = 0; i < OPS_PER_THREAD; i++) {
			double op = Math.random();
			if (op < PUSH_THRESHOLD) {
				int n = (int)(Math.random() * 10);
				stack.pushBack(n);
				pushQueue.add(n);
				System.out.println("Thread " + Thread.currentThread().getId() + " pushed " + n);
			} else {
				Integer n = stack.popBack();
				if (n != null) popQueue.add(n);
				System.out.println("Thread " + threadNum + " popped " + n);
			}
		}
	}
	
}
