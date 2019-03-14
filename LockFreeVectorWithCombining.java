import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import LockFreeVector.Descriptor;
import LockFreeVector.WriteDescriptor;

public class LockFreeVectorWithCombining<T> {

	/*
	 * Based on Walulya at al.'s lock-free vector paper.
	 */

	static final int FBS = 2; // First bucket size; can be any power of 2.
	static final int QSize = -1; // Size of the bounded combining queue.
	AtomicReference<Descriptor<T>> desc;
	AtomicReferenceArray<AtomicReferenceArray<T>> vals;
	AtomicReference<Queue<T>> batch;
	ThreadLocal<ThreadInfo> threadInfoGlobal;

	public LockFreeVectorWithCombining() {
		desc = new AtomicReference<Descriptor<T>>(new Descriptor<T>(0, null, null));
		// You need to do this cuz Java is dumb and won't let you make generic arrays.
		vals = new AtomicReferenceArray<AtomicReferenceArray<T>>(32);
		vals.getAndSet(0, new AtomicReferenceArray<T>(FBS));
	}

	void reserve(int newSize) {
		// The -1 is used because the `highestBit(x) - highestBit(y)` math finds the bucket for a 
		// given index. Since we're checking sizes, we only need size-1 indexes.

		// The index of the largest in-use bucket.
		int i = highestBit(desc.get().size + FBS - 1) - highestBit(FBS);
		if (i < 0) i = 0;

		// Add new buckets until we have enough buckets for newSize elements.
		while (i < highestBit(newSize + FBS - 1) - highestBit(FBS)) {
			i++;
			allocateBucket(i);
		}
	}

	void pushBack(T newElement) {
		// vector??
		boolean batchExists = false, help = false;
		Descriptor<T> currDesc, newDesc;
		ThreadInfo threadInfo = threadInfoGlobal.get();
		while (true) {
			currDesc = desc.get();

			completeWrite(currDesc.writeOp);
			
			if (currDesc.batch != null) {
				combine(threadInfo, currDesc, true);
			}

			// Create a new Descriptor and WriteDescriptor.
			WriteDescriptor<T> writeOp = new WriteDescriptor<T>(readAt(currDesc.size), newElement, 
					currDesc.size);
			newDesc = new Descriptor<T>(currDesc.size + 1, writeOp, OpType.PUSH);

			// Determine which bucket this element will go in.
			int bucketIdx = highestBit(currDesc.size + FBS) - highestBit(FBS);
			// If the appropriate bucket doesn't exist, create it.
			if (vals.get(bucketIdx) == null) allocateBucket(bucketIdx);

			if (batchExists || (threadInfo.q != null && threadInfo.q == batch.get())) {
				if (addToBatch(threadInfo, writeOp)) {
					return;
				}
				newDesc = new Descriptor<T>(currDesc.size, null, null);
				help = true;
				threadInfo.q = null;
			}

			if (desc.compareAndSet(currDesc, newDesc)) {
				if (newDesc.batch != null) {
					combine(threadInfo, newDesc, true);
					if (help) {
						help = false;
						continue;
					}
				}
				break;
			} else {
				batchExists = true;
			}
		}

		completeWrite(newDesc.writeOp);
	}

	T popBack() {
		// vector??
		boolean batchExists = false, help = false;
		Descriptor<T> currDesc, newDesc;
		ThreadInfo threadInfoLocal = threadInfoGlobal.get();
		T elem;
		while (true) {
			currDesc = desc.get();

			completeWrite(currDesc.writeOp);
			
			if (currDesc.batch != null) {
				combine(threadInfoLocal, currDesc, true);
			}
			
			if (currDesc.size == 0 && batch == null) return null; // There's nothing to pop.
			elem = readAt(currDesc.size - 1);

			// Create a new Descriptor and WriteDescriptor.
			newDesc = new Descriptor<T>(currDesc.size - 1, null, OpType.POP);
			newDesc.batch = batch.get();
			newDesc.offset = currDesc.size;

			if (desc.compareAndSet(currDesc, newDesc)) {
				if (newDesc.batch != null && newDesc.batch == batch) {
					threadInfo.q.closed = true;
					elem = combine(threadInfoLocal, newDesc, false);
				} else {
					markNode(currDesc.size);
				}
				break;
			}
		}

		return elem;
	}
	
	boolean addToBatch(ThreadInfo threadInfo, WriteDescriptor descr) {
		Queue queue = batch.get();
		if (threadInfo.q == null) {
			Queue newQ = new Queue(desc.get());
			if (batch.compareAndSet(queue, newQ)) {
				return true;
			}
		}
		
		threadInfo.q = batch.get();
		if (threadInfo.q == null || threadInfo.q.closed) {
			descr.batch = threadInfo.q;
			return false;
		}
		
		int ticket = queue.tail.getAndAdd(1);
		if (ticket >= QSize) {
			threadInfo.q.closed = true;
			descr.batch = queue;
			return false;
		}
		
		if (!queue.items.compareAndSet(ticket, null, descr)) {
			return false;
		}
		
		Queue newQ = new Batch(descr);
		return true;
	}
	
	T combine(WriteDescriptor descr, boolean helper) {
		Queue<T> q = batch.get();
		
		if (q == null || !q.closed) { // the paper has an AND here, which I don't think makes sense?
			return null; // queue not closed
		}
		
		while (true) {
			Head head = q.head.get();
			int headIndex = head.index, headCount = head.count;
			
			int bucketIdx = highestBit(descr.offset + currCount + FBS) - highestBit(FBS);
			if (vals.get(bucketIdx) == null) allocateBucket(bucketIdx);
			
			T oldValue = readAt(descr.offset + headCount);
			int ticket = headIndex;
			if (ticket == q.tail.get() || ticket == QSize) {
				break;
			}
			
			// linearize with push operation
			if (q.items.compareAndSet(ticket, null, ???)) {
				Head newHead = new Head(headIndex + 1, headCount); // update head
				q.tail.compareAndSet(head, newHead);
				continue;
			}
			
			if (q.items.get(ticket) == ???) { // gaps
				Head newHead = new Head(headIndex + 1, headCount); 
				q.tail.compareAndSet(head, newHead);
				continue;
			}
			
			writeOp = q.items.get(ticket);
			if (!writeOp.pending) { // update head
				Head newHead = new Head(headIndex + 1, headCount+1);
				q.tail.compareAndSet(head, newHead);
				continue;
			}
			
			if (writeOp.pending && q.head.get().index == headIndex && q.head.get().count == headCount) {
				int temp = descr.offset + headCount;
				vals.get(firstIdx(temp)).compareAndSet(secondIdx(temp), oldValue, writeOp.newValue);
			}
			
			Head newHead = new Head(headIndex + 1, headCount+1);
			q.head.compareAndSet(head, newHead);
			writeOp.pending = true;
		}
		
		int newSize = descr.offset + headCount;
		if (descr.opType == OpType.POP) {
			newSize--;
		}
		
		Descriptor newDesc = new Descriptor(newSize, null);
		desc.compareAndSet(descr, newDesc);
		
		if (!helper) {
			int index = descr.offset + headCount;
			T elem = readAt(index);
			markNode(index);
			return elem;
		}
		
		return null;
	}

	// New method, not in the paper's spec.
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
		if (desc.get().writeOp.pending) { // A pending pushBack().
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
			// Do nothing, and let the GC free newBucket. (Another thread allocated the bucket or 
			// it already existed.)
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

	// Returns the index of the highest one bit.
	private int highestBit(int n) {
		return Integer.numberOfTrailingZeros(Integer.highestOneBit(n));
	}

	private static class Descriptor<T> {
		int size, offset;
		WriteDescriptor<T> writeOp;
		Queue batch;
		OpType opType;

		Descriptor(int _size, WriteDescriptor<T> _writeOp, OpType _opType) {
			size = _size;
			writeOp = _writeOp;
			opType = _opType;
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

	private static class Queue<T> {
		boolean closed;
		AtomicReferenceArray<T> items;
		AtomicInteger tail;
		AtomicReference<Head> head;
	}
	
	private static class Head {
		int index, count;
	}

	private static enum OpType {
		PUSH, POP;
	}

	private static class ThreadInfo {
		// vector??
		Queue q, batch;
		int offset;
	}
}

