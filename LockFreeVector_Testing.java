
public class LockFreeVector_Testing {

	public static void main(String[] args) {
		LockFreeVector<Integer> vec = new LockFreeVector<>();
//		vec.reserve(10);
//		vec.writeAt(5, 100);
		int size = 20;
		for (int i = 0; i < size; i++) {
			int n = (int)(Math.random() * 200);
			vec.pushBack(n);
			System.out.println("push " + n);
		}
		for (int i = 0; i < size; i++) System.out.println("pop " + vec.popBack());
		System.out.println(vec.popBack());
	}
}
