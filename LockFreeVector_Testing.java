
public class LockFreeVector_Testing {

	public static void main(String[] args) {
		LockFreeVector<Integer> vec = new LockFreeVector<>();
//		vec.reserve(10);
//		vec.writeAt(5, 100);
		for (int i = 0; i < 10; i++) vec.pushBack((int)(Math.random() * 200));
		for (int i = 0; i < 10; i++) System.out.println(vec.popBack());
		System.out.println(vec.popBack());
	}
}
