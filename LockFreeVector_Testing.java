
public class LockFreeVector_Testing {
	
	/*
	 * This class is all very disorganized and haphazard.
	 */

	public static void main2(String[] args) {
		LockFreeVector<Integer> vec = new LockFreeVector<>();
//		vec.reserve(10);
//		vec.writeAt(5, 100);
//		int size = 20;
//		for (int i = 0; i < size; i++) {
//			int n = (int)(Math.random() * 200);
//			vec.pushBack(n);
//			System.out.println("push " + n);
//		}
//		for (int i = 0; i < size; i++) System.out.println("pop " + vec.popBack());
//		System.out.println(vec.popBack());
		
	}
	
	static final int NUM_THREADS = 8, NUM_OPS = 100, OPS_PER_THREAD = NUM_OPS / NUM_THREADS;
	// Push: [0, PUSH_THRESHOLD), pop: [PUSH_THRESHOLD, POP_THRESHOLD), size: [POP_THRESHOLD, 1)
	static final double PUSH_THRESHOLD = 0.5, POP_THRESHOLD = 0.75;
	public static void main(String[] args) throws InterruptedException {
		Prog_2_LockFreeStack<Integer> stack = new Prog_2_LockFreeStack<>();
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
				", OPS_PER_THREAD: " + OPS_PER_THREAD + ", numOps: " + stack.getNumOps());
		System.out.println("Size: " + stack.size());
	}
	
	static void useTheStack(Prog_2_LockFreeStack<Integer> stack, int threadNum) {
		for (int i = 0; i < OPS_PER_THREAD; i++) {
			double op = Math.random();
			if (op < PUSH_THRESHOLD) {
				int n = (int)(Math.random() * 10000);
				stack.push(n);
				System.out.println("Thread " + threadNum + " pushed " + n);
			} else if (op < POP_THRESHOLD) {
				Integer n = stack.pop();
				System.out.println("Thread " + threadNum + " popped " + n);
			} else {
				System.out.println("Thread " + threadNum + " sized " + stack.size());
			}
		}
	}
}
