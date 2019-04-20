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
	 * I also converted at() into two functions: getBucket() and getIdxWithinBucket(). at() returns 
	 * a pointer to the location in the array, which is impossible in Java. But combining the two 
	 * new functions gets you the same functionality.
	 * 
	 * Additionally, I added a peek() method.
	 * 
	 * How the binary math works:
	 * 		getBucket(): The index of the bucket to use is the index of the highest one bit 
	 * 			(accounting for the FBS), ie. the largest power of 2 in the binary representation 
	 * 			of i.
	 * 		getIdxWithinBucket(): The index within the bucket is i, with the first one bit turned 
	 * 			off (since that bit is used to determine which bucket to use).
	 * 
	 * Possible issue: Does the ABA problem occur with cached, boxed primitives? eg. for Integers, 
	 * two numbers will be the same object if their value is on [-128, 127] (and they were 
	 * autoboxed from ints).
	 */

	static final int FBS = 2; // First bucket size; can be any power of 2.
	AtomicReference<Descriptor<T>> desc;
	AtomicReferenceArray<AtomicReferenceArray<T>> vals;

	public LockFreeVector() {
		desc = new AtomicReference<Descriptor<T>>(new Descriptor<T>(0, null));
		vals = new AtomicReferenceArray<AtomicReferenceArray<T>>(32);
		vals.getAndSet(0, new AtomicReferenceArray<T>(FBS));
	}
	
	public LockFreeVector(int size) {
		this();
		reserve(size);
		desc.get().size = size;
	}

	void reserve(int newSize) {
		// The -1 is used because getBucket() finds the bucket for a given index. Since we're 
		// checking sizes, we only need to allocate size-1 indexes.
		
		// The index of the largest in-use bucket.
		int i = getBucket(desc.get().size - 1);
		if (i < 0) i = 0;
		
		// Add new buckets until we have enough buckets for newSize elements.
		while (i < getBucket(newSize - 1)) {
			i++;
			allocateBucket(i);
		}
	}

	void pushBack(T newElement) {
		Descriptor<T> currDesc, newDesc;
		// Run until we successfully change the descriptor.
		do {
			currDesc = desc.get();
			
			// Complete any pending operation of the old descriptor.
			completeWrite(currDesc.writeOp);
			
			// Determine which bucket this element will go in.
			int bucketIdx = highestBit(currDesc.size + FBS) - highestBit(FBS);
			// If the appropriate bucket doesn't exist, create it.
			if (vals.get(bucketIdx) == null) allocateBucket(bucketIdx);
			
			// Create a new Descriptor and WriteDescriptor.
			WriteDescriptor<T> writeOp = new WriteDescriptor<T>(readAt(currDesc.size), newElement, 
					currDesc.size);
			newDesc = new Descriptor<T>(currDesc.size + 1, writeOp);
		} while (!desc.compareAndSet(currDesc, newDesc));

		// Complete the pending write (assuming nobody else has).
		completeWrite(newDesc.writeOp);
	}

	T popBack() {
		Descriptor<T> currDesc, newDesc;
		T elem;
		// Run until we successfully change the descriptor.
		do {
			currDesc = desc.get();
			
			// Complete any pending operation of the old descriptor.
			completeWrite(currDesc.writeOp);
			
			if (currDesc.size == 0) return null; // There's nothing to pop.
			elem = readAt(currDesc.size - 1);
			
			// Create a new Descriptor.
			newDesc = new Descriptor<T>(currDesc.size - 1, null);
		} while (!desc.compareAndSet(currDesc, newDesc));
		
		return elem;
	}
	
	T peek() {
		Descriptor<T> currDesc = desc.get();
		completeWrite(currDesc.writeOp); // Complete any pending push.
		if (currDesc.size == 0) return null;
		else return readAt(currDesc.size - 1);
	}

	void writeAt(int idx, T newValue) {
		vals.get(getBucket(idx)).set(getIdxWithinBucket(idx), newValue);
	}

	T readAt(int idx) {
		return vals.get(getBucket(idx)).get(getIdxWithinBucket(idx));
	}

	int size() {
		Descriptor<T> currDesc = desc.get();
		int size = currDesc.size;
		if (currDesc.writeOp != null && currDesc.writeOp.pending) { // A pending pushBack().
			size--;
		}
		return size;
	}

	// Finish a pending write operation.
	private void completeWrite(WriteDescriptor<T> writeOp) {
		if (writeOp != null && writeOp.pending) {
			// We don't need to loop until it succeeds, because a failure means some other thread
			// completed it for us.
			vals.get(getBucket(writeOp.idx)).compareAndSet(getIdxWithinBucket(writeOp.idx), 
					writeOp.oldValue, writeOp.newValue);
			writeOp.pending = false;
		}
	}

	// Create a new bucket.
	private void allocateBucket(int bucketIdx) {
		int bucketSize = 1 << (bucketIdx + highestBit(FBS));
		AtomicReferenceArray<T> newBucket = new AtomicReferenceArray<T>(bucketSize);
		if (!vals.compareAndSet(bucketIdx, null, newBucket)) {
			// Do nothing, and let the GC free newBucket. (Another thread allocated the bucket or 
			// it already existed.)
		}
	}

	// Returns the index of the bucket for i (level zero of the array).
	private int getBucket(int i) {
		int pos = i + FBS;
		int hiBit = highestBit(pos);
		return hiBit - highestBit(FBS);
	}
	// Returns the index within the bucket for i (level one of the array).
	private int getIdxWithinBucket(int i) {
		int pos = i + FBS;
		int hiBit = highestBit(pos);
		return pos ^ (1 << hiBit);
	}

	// Returns the index of the highest one bit. eg. highestBit(8) = 3
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

