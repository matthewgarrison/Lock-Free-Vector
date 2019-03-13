import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class LockFreeVector<T> {
	
	/*
	 * Based on Dechev at al.'s lock-free vector paper.
	 * 
	 * The descriptor is used because pushback needs to update several things (size and an element 
	 * at an index) in one CAS. The descriptor allows us to change size immediately, then complete 
	 * the write afterward (including having a different thread do it, if necessary).
	 * 
	 * popback doesn't need a WriteDescriptor because we don't bother to set the removed element 
	 * to null. Therefore, the entire operation just consists of changing the size. That's also 
	 * why there's not a completeWrite() call outside of the do-while loop, like there is in 
	 * pushBack() (there's no write to complete).
	 * 
	 * Note: The Descriptor in the paper includes a reference counter used by their memory 
	 * management scheme. I have omitted this.
	 * 
	 * I also converted at() into two functions: firstIdx() and secondIdx(). at() returns a pointer 
	 * to the location in the array, which is impossible in Java. But combining the two new 
	 * functions gets you the same functionality.
	 * 
	 * How the binary math works:
	 * 		firstIdx(): The index of the bucket to use is the index of the highest on bit (accounting
	 * 			for the FBS), ie. the largest power of 2 in the binary representation of i.
	 * 		secondIdx(): The index within the bucket is i, with the first on bit turned off (since 
	 * 			(that bit is used to determine which bucket to use).
	 */

	static final int FBS = 2; // First bucket size; can be any power of 2.
	AtomicReference<Descriptor<T>> desc;
	AtomicReferenceArray<AtomicReferenceArray<T>> vals;

	public LockFreeVector() {
		desc = new AtomicReference<Descriptor<T>>(new Descriptor<T>(0, null));
		// You need to do this cuz Java is dumb and won't let you make generic arrays.
		vals = new AtomicReferenceArray<AtomicReferenceArray<T>>(32);
		vals.getAndSet(0, new AtomicReferenceArray<T>(FBS));
	}

	void reserve(int newSize) {
		int i = highestBit(desc.get().size + FBS - 1) - highestBit(FBS);
		if (i < 0) i = 0;
		while (i < highestBit(newSize + FBS - 1) - highestBit(FBS)) {
			i++;
			allocateBucket(i);
		}
	}

	void pushBack(T newElement) {
		Descriptor<T> currDesc, newDesc;
		do { // Run until we successfully change the descriptor.
			currDesc = desc.get();
			// Complete any pending operation of the old descriptor.
			completeWrite(currDesc.writeOp);
			// Determine which bucket this element will go in.
			int bucketIdx = highestBit(currDesc.size + FBS) - highestBit(FBS);
			// If the appropriate bucket doesn't exist, create it.
			if (vals.get(bucketIdx) == null) allocateBucket(bucketIdx);
			// Create a new Descriptor and WriteDescriptor.
			WriteDescriptor<T> writeOp = new WriteDescriptor<T>(readAt(currDesc.size), newElement, currDesc.size);
			newDesc = new Descriptor<T>(currDesc.size + 1, writeOp);
		} while (!desc.compareAndSet(currDesc, newDesc));

		// Complete the pending write (assuming nobody else has).
		completeWrite(newDesc.writeOp);
	}

	T popBack() {
		Descriptor<T> currDesc, newDesc;
		T elem;
		do { // Run until we successfully change the descriptor.
			currDesc = desc.get();
			// Complete any pending operation of the old descriptor.
			completeWrite(currDesc.writeOp);
			if (currDesc.size == 0) return null; // There's nothing to pop.
			elem = readAt(currDesc.size - 1);
			// Create a new Descriptor. We don't need a WriteDescriptor because we don't bother to 
			// set the removed element to null. Therefore, the entire operation just consists of 
			// changing the size. That's also why there's not a completeWrite() call outside of the 
			// do-while loop, like there is in pushBack() (there's no write to complete).
			newDesc = new Descriptor<T>(currDesc.size - 1, null);
		} while (!desc.compareAndSet(currDesc, newDesc));
		
		return elem;
	}
	
	// New method, not in paper's spec.
	T peek() {
		Descriptor<T> currDesc = desc.get();
		completeWrite(currDesc.writeOp); // Complete any pending push.
		if (currDesc.size == 0) return null;
		else return readAt(currDesc.size - 1);
	}

	void writeAt(int idx, T newValue) {
		vals.get(firstIdx(idx)).set(secondIdx(idx), newValue);
	}

	T readAt(int idx) {
		return vals.get(firstIdx(idx)).get(secondIdx(idx));
	}

	int size() {
		int size = desc.get().size;
		if (desc.get().writeOp.pending) { // A pending pushback().
			size--;
		}
		return size;
	}

	// Finish a pending write operation.
	private void completeWrite(WriteDescriptor<T> writeOp) {
		if (writeOp != null && writeOp.pending) {
			// We don't need to loop until it succeeds, because a failure means some other thread
			// completed it for us.
			vals.get(firstIdx(writeOp.idx)).compareAndSet(secondIdx(writeOp.idx), 
					writeOp.oldValue, writeOp.newValue);
			writeOp.pending = false;
		}
	}

	// Create a new bucket.
	private void allocateBucket(int bucketIdx) {
		int bucketSize = 1 << (bucketIdx + highestBit(FBS));
		AtomicReferenceArray<T> newBucket = new AtomicReferenceArray<T>(bucketSize);
		if (!vals.compareAndSet(bucketIdx, null, newBucket)) {
			// Do nothing, and let the GC free newBucket. (Another thread allocated the bucket.)
		}
	}

	// Returns the first index (level zero) into the array.
	private int firstIdx(int i) {
		int pos = i + FBS;
		int hiBit = highestBit(pos);
		return hiBit - highestBit(FBS);
	}
	// Returns the second index (level one) into the array.
	private int secondIdx(int i) {
		int pos = i + FBS;
		int hiBit = highestBit(pos);
		return pos ^ (1 << hiBit);
	}

	private int highestBit(int n) {
		return Integer.numberOfTrailingZeros(Integer.highestOneBit(n));
	}

	private static class Descriptor<T> {
		int size;
		WriteDescriptor<T> writeOp;

		Descriptor(int _size, WriteDescriptor<T> _writeOp) {
			size = _size;
			writeOp = _writeOp;
		}
	}

	private static class WriteDescriptor<T> {
		T oldValue, newValue;
		int idx;
		boolean pending;

		WriteDescriptor(T _oldV, T _newV, int _idx) {
			oldValue = _oldV;
			newValue = _newV;
			pending = true;
			idx = _idx;
		}
	}
}

