import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import LockFreeVector.Descriptor;
import LockFreeVector.WriteDescriptor;

public class LockFreeVectorWithCombining<T> {

	/*
	 * Based on Walulya at al.'s lock-free vector paper.
	 * 
	 * I think I can do CAS(e, u, false, false) - if a node is ever marked, then we can let the 
	 * CAS fail cuz the value doesn't matter.
	 */
	
	static final int FBS = 2; // First bucket size; can be any power of 2.
	static final int QSize = -1; // Size of the bounded combining queue.
	AtomicReference<Descriptor<AtomicMarkableReference<T>>> desc;
	AtomicReferenceArray<AtomicReferenceArray<AtomicMarkableReference<T>>> vals;
	AtomicReference<Queue<AtomicMarkableReference<T>>> batch;
	ThreadLocal<ThreadInfo<T>> threadInfoGlobal;
	AtomicMarkableReference<T> SENTINEL_ONE, SENTINEL_TWO;

	public LockFreeVectorWithCombining() {
		desc = new AtomicReference<Descriptor<AtomicMarkableReference<T>>>(new Descriptor<>(0, null, null));
		// You need to do this cuz Java is dumb and won't let you make generic arrays.
		vals = new AtomicReferenceArray<AtomicReferenceArray<AtomicMarkableReference<T>>>(32);
		vals.getAndSet(0, new AtomicReferenceArray<AtomicMarkableReference<T>>(FBS));
		SENTINEL_ONE = new AtomicMarkableReference<>(null, false);
		SENTINEL_TWO = new AtomicMarkableReference<>(null, true);
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
		Descriptor<AtomicMarkableReference<T>> currDesc, newDesc;
		ThreadInfo<T> threadInfo = threadInfoGlobal.get();
		while (true) {
			currDesc = desc.get();

			completeWrite(currDesc.writeOp);
			
			// "a concurrent Combine operation is on-going, and the thread tries to help complete 
			// this operation."
			if (currDesc.batch != null) {
				combine(threadInfo, currDesc, true);
			}

			// Create a new Descriptor and WriteDescriptor.
			WriteDescriptor<AtomicMarkableReference<T>> writeOp = new WriteDescriptor<AtomicMarkableReference<T>>(readRefAt(currDesc.size), newElement, 
					currDesc.size);
			newDesc = new Descriptor<AtomicMarkableReference<T>>(currDesc.size + 1, writeOp, OpType.PUSH);

			// Determine which bucket this element will go in.
			int bucketIdx = getBucket(currDesc.size);
			// If the appropriate bucket doesn't exist, create it.
			if (vals.get(bucketIdx) == null) allocateBucket(bucketIdx);

			//
			if (batchExists || (threadInfo.q != null && threadInfo.q == batch.get())) {
				if (addToBatch(threadInfo, writeOp)) {
					return;
				}
				newDesc = new Descriptor<AtomicMarkableReference<T>>(currDesc.size, null, null);
				help = true;
				threadInfo.q = null;
			}

			// try the normal compare and set
			if (desc.compareAndSet(currDesc, newDesc)) {
				if (newDesc.batch != null) {
					combine(threadInfo, newDesc, true);
					if (help) {
						help = false;
						continue;
					}
				}
				break; // we're done
			} else {
				// the thread adds the operation to the combining queue
				batchExists = true;
			}
		}

		completeWrite(newDesc.writeOp);
	}

	T popBack() {
		// vector??
		boolean batchExists = false, help = false;
		Descriptor<AtomicMarkableReference<T>> currDesc, newDesc;
		ThreadInfo<T> threadInfo = threadInfoGlobal.get();
		T elem;
		while (true) {
			currDesc = desc.get();

			completeWrite(currDesc.writeOp);
			
			if (currDesc.batch != null) {
				combine(threadInfo, currDesc, true);
			}
			
			if (currDesc.size == 0 && batch == null) return null; // There's nothing to pop.
			elem = readAt(currDesc.size - 1);

			// Create a new Descriptor and WriteDescriptor.
			newDesc = new Descriptor<>(currDesc.size - 1, null, OpType.POP);
			newDesc.batch = batch.get();
			newDesc.offset = currDesc.size;

			if (desc.compareAndSet(currDesc, newDesc)) {
				if (newDesc.batch != null && newDesc.batch == batch.get()) {
					threadInfo.q.closed = true;
					elem = combine(threadInfo, newDesc, false);
				} else {
					markNode(currDesc.size);
				}
				break;
			}
		}

		return elem;
	}
	
	// I call the WriteDescriptor writeOp for consistent naming (the paper calls it descr, which is 
	// confusing).
	boolean addToBatch(ThreadInfo<T> threadInfo, WriteDescriptor<T> descr) {
		Queue<T> queue = batch.get();
		if (queue == null) { // check if the vector has a combining queue
			Queue<T> newQ = new Queue<T>(descr);
			if (batch.compareAndSet(queue, newQ)) {
				return true;
			}
		}
		
		queue = batch.get(); // in case a different thread CASed before us
		if (queue == null || queue.closed) {
			// we don't add descr, cuz it's closed/non-existant
			descr.batch = queue;
			return false;
		}
		
		int ticket = queue.tail.getAndAdd(1); // where we'll insert
		if (ticket >= QSize) {
			// queue is full, so close it and return a failure
			queue.closed = true;
			descr.batch = queue;
			return false;
		}
		
		if (!queue.items.compareAndSet(ticket, null, descr)) { // add it to the queue
			return false; // we failed, someone stole our spot
		}
		
		Queue<T> newQ = new Batch(descr); // what's the point of this? we don't do anything with it
		return true;
	}
	
	T combine(ThreadInfo<T> threadInfo, Descriptor<T> descr, boolean helper) {
		Queue<T> q = batch.get();
		
		if (q == null || !q.closed) { // the paper has an AND here, which I don't think makes sense?
			return null; // queue not closed, so somebody else already combined
		}
		
		// we dequeue items and add them to the vector
		while (true) {
			// headIndex is the dequeue index
			Head head = q.head.get();
			int headIndex = head.index, headCount = head.count;
			
			int bucketIdx = getBucket(descr.offset + currCount);
			if (vals.get(bucketIdx) == null) allocateBucket(bucketIdx);
			
			T oldValue = readAt(descr.offset + headCount);
			int ticket = headIndex;
			if (ticket == q.tail.get() || ticket == QSize) {
				break;
			}
			
			// linearize with push operation
			// 1) the corresponding AddToBatch operation has not completed adding item to the combining queue.
			if (q.items.compareAndSet(ticket, SENTINEL_ONE, SENTINEL_TWO)) {
				Head newHead = new Head(headIndex + 1, headCount); // update head
				q.tail.compareAndSet(head, newHead);
				continue;
			}
			
			// 3) the node value was by an interfering Combine operation before the AddToBatch 
			// succeeded (the AddToBatch failed).
			if (q.items.get(ticket) == SENTINEL_TWO) { // gaps
				Head newHead = new Head(headIndex + 1, headCount); 
				q.tail.compareAndSet(head, newHead);
				continue;
			}
			
			// 2) The node value is a write descriptor which implies that the AddToBatch 
			// operation completed successfully.
			writeOp = q.items.get(ticket);
			if (!writeOp.pending) { // update head
				Head newHead = new Head(headIndex + 1, headCount+1);
				q.tail.compareAndSet(head, newHead);
				continue;
			}
			
			if (writeOp.pending && q.head.get().index == headIndex && q.head.get().count == headCount) {
				int temp = descr.offset + headCount;
				vals.get(getBucket(temp)).compareAndSet(secondIdx(temp), oldValue, writeOp.newValue);
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
		Descriptor<AtomicMarkableReference<T>> currDesc = desc.get();
		completeWrite(currDesc.writeOp); // Complete any pending push.
		if (currDesc.size == 0) return null;
		else return readAt(currDesc.size - 1);
	}

	// they modified this method, don't forget to do that
	void writeAt(int idx, T newValue) {
		vals.get(getBucket(idx)).get(secondIdx(idx)).set(newValue, false);
	}

	// they modified this method, don't forget to do that
	T readAt(int idx) {
		return vals.get(getBucket(idx)).get(secondIdx(idx)).getReference();
	}
	private AtomicMarkableReference<T> readRefAt(int idx) {
		return vals.get(getBucket(idx)).get(secondIdx(idx));
	}
	
	int size() {
		int size = desc.get().size;
		if (desc.get().writeOp.pending) { // A pending pushBack().
			size--;
		}
		return size;
	}
	
	// how to do this in java? AtomicMarkableReference<Integer>? look at LockFreeList
	private void markNode(int idx) {
		int[] temp = new int[1];
		temp[-1] = 0;
	}
	
	// Finish a pending write operation.
	private void completeWrite(WriteDescriptor<AtomicMarkableReference<T>> writeOp) {
		if (writeOp != null && writeOp.pending) {
			// We don't need to loop until it succeeds, because a failure means some other thread
			// completed it for us.
			vals.get(getBucket(writeOp.idx)).compareAndSet(secondIdx(writeOp.idx),
					writeOp.newValue, writeOp.oldValue);
			writeOp.pending = false;
		}
	}

	// Create a new bucket.
	private void allocateBucket(int bucketIdx) {
		int bucketSize = 1 << (bucketIdx + highestBit(FBS));
		AtomicReferenceArray<AtomicMarkableReference<T>> newBucket = new AtomicReferenceArray<>(bucketSize);
		if (!vals.compareAndSet(bucketIdx, null, newBucket)) {
			// Do nothing, and let the GC free newBucket. (Another thread allocated the bucket or 
			// it already existed.)
		}
		for (int i = 0; i < bucketSize; i++) {
			vals.get(bucketIdx).set(i, SENTINEL_ONE);
		}
	}

	// Returns the first index (level zero) into the array.
	private int getBucket(int i) {
		int pos = i + FBS;
		int hiBit = highestBit(pos);
		return hiBit - highestBit(FBS);
	}
	// Returns the second index (level one) into the array.
	// change to something like getidxinbucket
	private int secondIdx(int i) {
		int pos = i + FBS;
		int hiBit = highestBit(pos);
		return pos ^ (1 << hiBit);
	}

	// Returns the index of the highest one bit.
	private int highestBit(int n) {
		return Integer.numberOfTrailingZeros(Integer.highestOneBit(n));
	}

	private static class Descriptor<E> {
		int size, offset;
		WriteDescriptor<E> writeOp;
		Queue<E> batch;
		OpType opType;

		Descriptor(int _size, WriteDescriptor<E> _writeOp, OpType _opType) {
			size = _size;
			writeOp = _writeOp;
			opType = _opType;
		}
	}

	private static class WriteDescriptor<E> {
		E oldValue, newValue;
		int idx;
		boolean pending;

		WriteDescriptor(E _oldV, E _newV, int _idx) {
			oldValue = _oldV;
			newValue = _newV;
			pending = true;
			idx = _idx;
		}
	}

	private static class Queue<E> {
		boolean closed;
		AtomicReferenceArray<WriteDescriptor<E>> items;
		AtomicInteger tail;
		AtomicReference<Head> head;
		
		Queue(WriteDescriptor<E> firstElement) {
			items = new AtomicReferenceArray<>(QSize);
			items.set(0, firstElement);
			closed = false;
			tail = new AtomicInteger(1);
			head = new AtomicReference<Head>(new Head(0, 0));
		}
	}
	
	private static class Head {
		// hcount indicates the number of successfully combined operations.
		int index, count;
		Head(int _index, int _count) {
			index = _index;
			count = _count;
		}
	}

	private static enum OpType {
		PUSH, POP;
	}

	private static class ThreadInfo<T> {
		// vector??
		Queue<T> q, batch;
		int offset;
	}
}

