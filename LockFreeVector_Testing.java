
public class LockFreeVector_Testing {
	
	/*
	 * This class is all very disorganized and gross. Please ignore.
	 */

	public static void main(String[] args) {
		LockFreeVectorWithCombining<Integer> vec = new LockFreeVectorWithCombining<>();
		vec.reserve(10);
		System.out.println("write 5,100: " + vec.writeAt(5, 100));
		System.out.println("read 5: " + vec.readAt(5));
		vec = new LockFreeVectorWithCombining<>(10);
		System.out.println("write 5,100: " + vec.writeAt(5, 100));
		System.out.println("read 5: " + vec.readAt(5));
		int size = 20;
		for (int i = 0; i < size; i++) {
			int n = (int)(Math.random() * 200);
			vec.pushBack(n);
			System.out.println("push " + n);
		}
		for (int i = 0; i < size; i++) System.out.println("pop " + vec.popBack());
		System.out.println("pop empty: " + vec.popBack());
	}
	
	public static void main2(String[] args) {
		LockFreeVector<Integer> vec = new LockFreeVector<>(10);
		for (int i = 0; i < 10; i++) System.out.println(vec.readAt(i));
		for (int i = 0; i < 10; i++) vec.writeAt(i, i*6);
		for (int i = 0; i < 10; i++) System.out.println(vec.readAt(i));
		vec.pushBack(9);
		System.out.println("size: " + vec.size());
		
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
//		
//		int size = 10;
//		for (int i = 0; i < size; i++) {
//			Integer n = (int)(Math.random() * 6);
//			if (n == 0) n = null;
//			vec.pushBack(n);
//			System.out.println("push " + n);
//		}
//		for (int i = 0; i < size; i++) System.out.println("read " + i + ": " + vec.readAt(i));
//		for (int i = 0; i < size; i++) System.out.println("pop " + vec.popBack());
//		System.out.println(vec.popBack());
	}
	
}
