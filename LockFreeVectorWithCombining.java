import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class LockFreeVectorWithCombining<T> {

	/*
	 * Based on Walulya at al.'s lock-free vector paper.
	 * 
	 * I think I can do CAS(e, u, false, false) - if a node is ever marked, then we can let the 
	 * CAS fail because the value doesn't matter (ie. the node was deleted).
	 * 
	 * A Combine operation is considered "ready" once the descriptor's batch value is set (this 
	 * done by AddToBatch or popback).
	 * 
	 * The paper doesn't recommend a specific QSize, so I picked mine fairly arbitrarily.
	 * 
	 * ThreadInfo
	 * 		- I renamed offset to size, for consistency with other naming.
	 * 		- The paper doesn't say when to update size (except in read() and write()), so I update 
	 * 		  it anywhere doing so doesn't require calling desc.get().size.
	 * 		- ThreadInfo has both a q and a batch, but batch doesn't appear to ever be used, so I omitted 
	 * 		  it.
	 * 
	 * Additionally, I added a peek() method.
	 * 
	 * [[Does each thread has it's own combining queue? If so, when does that get used vs. the 
	 * 		global combining queue?]]
	 */

	static final int FBS = 2; // First bucket size; can be any power of 2.
	static final int QSize = 16; // Size of the bounded combining queue.
	AtomicReference<Descriptor<AtomicMarkableReference<T>>> desc;
	AtomicReferenceArray<AtomicReferenceArray<AtomicMarkableReference<T>>> vals;
	AtomicReference<Queue<AtomicMarkableReference<T>>> batch;
	ThreadLocal<ThreadInfo<T>> threadInfoGlobal;
	WriteDescriptor<AtomicMarkableReference<T>> EMPTY_SLOT, FINISHED_SLOT;

	public LockFreeVectorWithCombining() {
		desc = new AtomicReference<Descriptor<AtomicMarkableReference<T>>>(new Descriptor<>(0, null, null));

		vals = new AtomicReferenceArray<AtomicReferenceArray<AtomicMarkableReference<T>>>(32);
		vals.getAndSet(0, new AtomicReferenceArray<AtomicMarkableReference<T>>(FBS));

		threadInfoGlobal = new ThreadLocal<>() {
			@Override protected ThreadInfo<T> initialValue() {
				return new ThreadInfo<T>();
			}
		};

		batch = new AtomicReference<>(null);

		EMPTY_SLOT = new WriteDescriptor<AtomicMarkableReference<T>>(null, null, -1);
		FINISHED_SLOT = new WriteDescriptor<AtomicMarkableReference<T>>(null, null, -2);
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
		boolean willAddToBatch = false, help = false;
		Descriptor<AtomicMarkableReference<T>> currDesc, newDesc;
		ThreadInfo<T> threadInfo = threadInfoGlobal.get();
		AtomicMarkableReference<T> newRef = new AtomicMarkableReference<>(newElement, false);
		while (true) {
			currDesc = desc.get();

			// Complete any pending operation.
			completeWrite(currDesc.writeOp);

			// If there's a current or ready Combine operation, this thread will help complete it.
			if (currDesc.batch != null) {
				combine(threadInfo, currDesc, true);
			}

			// Determine which bucket this element will go in.
			int bucketIdx = getBucket(currDesc.size);
			// If the appropriate bucket doesn't exist, create it.
			if (vals.get(bucketIdx) == null) allocateBucket(bucketIdx);

			// Create a new Descriptor and WriteDescriptor.
			WriteDescriptor<AtomicMarkableReference<T>> writeOp = new WriteDescriptor<AtomicMarkableReference<T>>
			(readRefAt(currDesc.size), newRef, currDesc.size);
			newDesc = new Descriptor<AtomicMarkableReference<T>>(currDesc.size + 1, writeOp, OpType.PUSH);

			// If our CAS failed (in a previous loop iteration) or this thread has already added 
			// items to the queue, then we'll try to add this operation to the queue. (Once we add 
			// one operation to the queue, we'll keep doing so until that queue closes.)
			if (willAddToBatch || (threadInfo.q != null && threadInfo.q == batch.get())) {
				if (addToBatch(threadInfo, newDesc, writeOp)) {
					return; // The operation was added to the queue, so we're done here.
				}
				// We couldn't add it to the queue.
				// [[Not sure why we set newDesc.writeOp to null - we didn't add it to the queue, 
				// so where did it go?]]
				newDesc = new Descriptor<AtomicMarkableReference<T>>(currDesc.size, null, null);
				help = true;
				threadInfo.q = null;
			}

			// Try the normal compare and set.
			if (desc.compareAndSet(currDesc, newDesc)) {
				if (newDesc.batch != null) {
					// AddToBatch set the descriptor's queue, which only happens when we're ready 
					// to combine.
					combine(threadInfo, newDesc, true);
					if (help) {
						help = false;
						continue;
					}
				}
				break; // We're done.
			} else {
				// The thread adds the operation to the combining queue (in the next loop iteration).
				willAddToBatch = true;
			}
		}

		completeWrite(newDesc.writeOp);
		threadInfo.size = newDesc.size;
	}

	T popBack() {
		Descriptor<AtomicMarkableReference<T>> currDesc, newDesc;
		ThreadInfo<T> threadInfo = threadInfoGlobal.get();
		T elem;
		while (true) {
			currDesc = desc.get();

			// Complete any pending operation
			completeWrite(currDesc.writeOp);

			// If there's a current or ready Combine operation, this thread will help complete it.
			if (currDesc.batch != null) {
				combine(threadInfo, currDesc, true);
			}
			if (currDesc.size == 0 && batch.get() == null) return null; // There's nothing to pop.

			elem = readAt(currDesc.size - 1);

			// Create a new Descriptor.
			newDesc = new Descriptor<>(currDesc.size - 1, null, OpType.POP);
			newDesc.batch = batch.get(); // This signals that the Combine operation should start.
			newDesc.offset = currDesc.size; // The size of the vector, without this pop.

			if (desc.compareAndSet(currDesc, newDesc)) {
				if (newDesc.batch != null && newDesc.batch == batch.get()) {
					// We need to execute any pending pushes before we can pop. Then we'll return 
					// the last element added to the vector by Combine.
					threadInfo.q.closed = true;
					elem = combine(threadInfo, newDesc, false);
				} else {
					// Mark the node as logically deleted.
					markNode(currDesc.size - 1);
				}
				break;
			}
		}

		threadInfo.size = newDesc.size;
		return elem;
	}

	boolean addToBatch(ThreadInfo<T> threadInfo, Descriptor<AtomicMarkableReference<T>> descr, 
			WriteDescriptor<AtomicMarkableReference<T>> writeOp) {
		Queue<AtomicMarkableReference<T>> queue = batch.get();
		// Check if the vector has a combining queue already. If not, we'll make one.
		if (queue == null) {
			Queue<AtomicMarkableReference<T>> newQ = new Queue<>(writeOp);
			if (batch.compareAndSet(queue, newQ)) {
				return true;
			}
		}

		queue = batch.get(); // In case a different thread CASed before us.
		if (queue == null || queue.closed) {
			// We don't add descr, because the queue is closed or non-existent.
			descr.batch = queue;
			descr.offset = descr.size - 1; // The size of the vector, without descr's push.
			return false;
		}

		int ticket = queue.tail.getAndAdd(1); // Where we'll insert into the queue.
		if (ticket >= QSize) {
			// The queue is full, so close it and return a failure.
			queue.closed = true;
			descr.batch = queue;
			descr.offset = descr.size - 1; // The size of the vector, without descr's push.
			return false;
		}

		if (!queue.items.compareAndSet(ticket, EMPTY_SLOT, writeOp)) { // Add it to the queue.
			return false; // We failed; someone stole our spot.
		}

		// [[What's the point of this? We don't do anything with it.]]
		Queue<AtomicMarkableReference<T>> newQ = new Queue<>(writeOp);

		// We successfully added the operation to the queue.
		return true;
	}

	T combine(ThreadInfo<T> threadInfo, Descriptor<AtomicMarkableReference<T>> descr, boolean 
			dontNeedToReturn) {
		Queue<AtomicMarkableReference<T>> q = batch.get();
		int headIndex, headCount;

		if (q == null || !q.closed) { // [[The paper has an AND here, which I don't think makes sense?]]
			return null; // The queue is not closed, so the combining phase is finished.
		}

		// We dequeue operations and execute them.
		while (true) {
			Head head = q.head.get();
			headIndex = head.index;
			headCount = head.count;

			// Determine which bucket this element will go in.
			// [[The paper uses a variable called cur_count, but that's not referenced anywhere else, 
			// so I think they mean headCount.]]
			int bucketIdx = getBucket(descr.offset + headCount);
			// If the appropriate bucket doesn't exist, create it.
			if (vals.get(bucketIdx) == null) allocateBucket(bucketIdx);

			AtomicMarkableReference<T> oldValue = readRefAt(descr.offset + headCount);
			int ticket = headIndex;
			if (ticket == q.tail.get() || ticket == QSize) {
				break; // We executed every operation in the queue.
			}

			// If our CAS succeeds, then the corresponding AddToBatch operation has not finished 
			// adding the item to the combining queue, so we just keep going.
			if (q.items.compareAndSet(ticket, EMPTY_SLOT, FINISHED_SLOT)) {
				Head newHead = new Head(headIndex + 1, headCount);
				// [[The paper updates tail here, but I'm pretty sure that's wrong.]]
				q.head.compareAndSet(head, newHead);
				continue;
			}

			// One of the others threads helping with this Combine operation succeeded in the CAS
			// (see above), so the AddToBatch failed and we keep going. 
			if (q.items.get(ticket) == FINISHED_SLOT) {
				Head newHead = new Head(headIndex + 1, headCount); 
				// [[The paper updates tail here, but I'm pretty sure that's wrong.]]
				q.head.compareAndSet(head, newHead);
				continue;
			}

			// The AddToBatch succeeded, so now we'll try to execute the WriteDescriptor's operation.
			WriteDescriptor<AtomicMarkableReference<T>> writeOp = q.items.get(ticket);
			if (!writeOp.pending) { // A different thread did it for us, so just update head.
				Head newHead = new Head(headIndex + 1, headCount+1);
				// [[The paper updates tail here, but I'm pretty sure that's wrong.]]
				q.head.compareAndSet(head, newHead);
				continue;
			}

			// Complete writeOp's pending operation.
			if (writeOp.pending && q.head.get().index == headIndex && q.head.get().count == headCount) {
				int temp = descr.offset + headCount;
				vals.get(getBucket(temp)).compareAndSet(getIdxWithinBucket(temp), oldValue, writeOp.newValue);
			}

			// Update head and mark writeOp as complete.
			Head newHead = new Head(headIndex + 1, headCount+1);
			q.head.compareAndSet(head, newHead);
			writeOp.pending = false;
		}

		// Set the size of the vector after all of the pushes are complete.
		int newSize = descr.offset + headCount;
		if (descr.opType == OpType.POP) {
			newSize--;
		}
		threadInfo.size = newSize;

		// Update the descriptor.
		Descriptor<AtomicMarkableReference<T>> newDesc = new Descriptor<AtomicMarkableReference<T>>(newSize, null, null);
		desc.compareAndSet(descr, newDesc);
		
		// Nullify the combining queue, so we are ready for next time.
		batch.compareAndSet(q, null);

		// This thread started the Combine and is executing a popback, so we need to return the last 
		// value we pushed. (If this Combine was started by a pushback or a different thread's 
		// popback, we don't return anything.)
		if (!dontNeedToReturn) {
			int index = descr.offset + headCount;
			T elem = readAt(index);
			markNode(index); // Mark the node as logically deleted.
			return elem;
		}

		return null;
	}

	T peek() {
		Descriptor<AtomicMarkableReference<T>> currDesc = desc.get();

		// If there's a current or ready Combine operation, this thread will help complete it.
		if (currDesc.batch != null) {
			ThreadInfo<T> threadInfo = threadInfoGlobal.get();
			combine(threadInfo, currDesc, true);
		}

		completeWrite(currDesc.writeOp); // Complete any pending operation.

		if (currDesc.size == 0) return null;
		else return readAt(currDesc.size - 1);
	}
	
	private boolean inBounds(int idx) {
		ThreadInfo<T> threadInfo = threadInfoGlobal.get();
		if (idx >= threadInfo.size) {
			// Update the local size to match the global descriptor's size.
			threadInfo.size = desc.get().size;
		}
		if (idx >= threadInfo.size) return false;
		if (vals.get(getBucket(idx)).get(getIdxWithinBucket(idx)) == null) return true;
		if (vals.get(getBucket(idx)).get(getIdxWithinBucket(idx)).isMarked()) {
			// Was logically deleted, which is considered out of bounds.
			return false;
		}
		return true;
	}

	boolean writeAt(int idx, T newValue) {
		if (!inBounds(idx)) return false;
		if (vals.get(getBucket(idx)).get(getIdxWithinBucket(idx)).compareAndSet(readAt(idx), 
				newValue, false, false)) {
			return true;
		}
		return false;
	}

	T readAt(int idx) {
		if (!inBounds(idx)) return null;
		AtomicMarkableReference<T> ref = readRefAt(idx);
		if (ref != null) return ref.getReference();
		else return null;
	}
	private AtomicMarkableReference<T> readRefAt(int idx) {
		// Does not perform bounds checking.
		return vals.get(getBucket(idx)).get(getIdxWithinBucket(idx));
	}

	int size() {
		Descriptor<AtomicMarkableReference<T>> currDesc = desc.get();
		int size = currDesc.size;

		// If there's a current or ready Combine operation, this thread will help complete it.
		if (currDesc.batch != null) {
			ThreadInfo<T> threadInfo = threadInfoGlobal.get();
			combine(threadInfo, currDesc, true);
		}

		// Take into account any pending WriteDescriptors.
		if (currDesc.writeOp != null && currDesc.writeOp.pending) {
			if (currDesc.opType == OpType.PUSH) size--;
			else size++;
		}
		return size;
	}

	private void markNode(int idx) {
		vals.get(getBucket(idx)).get(getIdxWithinBucket(idx)).attemptMark(readAt(idx), true);
	}

	// Finish a pending write operation.
	private void completeWrite(WriteDescriptor<AtomicMarkableReference<T>> writeOp) {
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
		AtomicReferenceArray<AtomicMarkableReference<T>> newBucket = new AtomicReferenceArray<>(bucketSize);
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

	// Returns the index of the highest one bit.
	private int highestBit(int n) {
		return Integer.numberOfTrailingZeros(Integer.highestOneBit(n));
	}

	private static class Descriptor<E> {
		// offset is the size of the vector at the start of the combining phase. (This is not the 
		// same as size, which is the size of the vector after writeOp, if present, has been 
		// completed.)
		int size, offset;
		WriteDescriptor<E> writeOp;
		Queue<E> batch;
		OpType opType;

		Descriptor(int _size, WriteDescriptor<E> _writeOp, OpType _opType) {
			size = _size;
			offset = -1;
			writeOp = _writeOp;
			opType = _opType;
			batch = null;
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

		Queue() {
			items = new AtomicReferenceArray<>(QSize);
			closed = false;
			tail = new AtomicInteger(1);
			head = new AtomicReference<Head>(new Head(0, 0));
		}

		Queue(WriteDescriptor<E> firstElement) {
			this();
			items.set(0, firstElement);
		}
	}

	private static class Head {
		// index is the index of the head point and count indicates the number of successfully 
		// combined operations.
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
		Queue<AtomicMarkableReference<T>> q;
		int size;

		public ThreadInfo() {
			q = new Queue<AtomicMarkableReference<T>>();
			size = 0;
		}
	}
}

