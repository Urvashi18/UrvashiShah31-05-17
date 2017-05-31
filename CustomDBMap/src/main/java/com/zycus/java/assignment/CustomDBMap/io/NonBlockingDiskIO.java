package com.zycus.java.assignment.CustomDBMap.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.zycus.java.assignment.CustomDBMap.Configuration;
import com.zycus.java.assignment.CustomDBMap.Record;

public class NonBlockingDiskIO extends BaseDiskIO implements DiskIO {
	private final BlockingQueue<ReadFuture> readQueue;
	private final Thread readerThread;
	private final ReaderTask readerTask;

	public NonBlockingDiskIO(Configuration config) {
		super(config, null);
		this.readQueue = new LinkedBlockingQueue<ReadFuture>();
		this.readerTask = new ReaderTask(readQueue);
		this.readerThread = new Thread(readerTask, "ReaderThread-" + config.getNumber());
		this.readerThread.start();
	}

	@Override
	public Record lookup(long location) {
		ReadFuture task = new ReadFuture(location);
		try {
			this.readQueue.put(task);
			return task.get();
		} catch (InterruptedException e) {
			throw newRuntimeException(e);
		} catch (ExecutionException e) {
			throw newRuntimeException(e);
		}
	}

	@Override
	public long write(Record r) {
		try {
			return doWrite(r, writer());
		} catch (IOException e) {
			throw newRuntimeException(e);
		}
	}

	@Override
	public void vacuum(RecordFilter filter) throws Exception {
		doVacuum(filter);
	}

	@Override
	public void update(Record record) {
		try {
			doUpdate(record);
		} catch (IOException e) {
			throw newRuntimeException(e);
		}
	}

	@Override
	public void update(Record... records) {
		try {
			doUpdate(records);
		} catch (IOException e) {
			throw newRuntimeException(e);
		}
	}

	@Override
	public void close() {
		readerTask.stop();
		super.close();
	}

	public class ReaderTask implements Runnable {
		private BlockingQueue<ReadFuture> readQueue;
		private AtomicBoolean shouldRun = new AtomicBoolean(true);

		public ReaderTask(BlockingQueue<ReadFuture> readQueue) {
			this.readQueue = readQueue;
		}

		@Override
		public void run() {
			while (shouldRun()) {
				try {
					List<ReadFuture> readFutures = new ArrayList<ReadFuture>();
					NonBlockingDiskIO.ReadFuture item = readQueue.poll(1000, TimeUnit.MILLISECONDS);
					if (item == null) {
						continue;
					}
					readFutures.add(item);
					readQueue.drainTo(readFutures);
					Collections.sort(readFutures);
					for (ReadFuture future : readFutures) {
						Record record = doLookup(future.getLocation());
						future.complete(record);
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		private boolean shouldRun() {
			return shouldRun.get();
		}

		public void stop() {
			this.shouldRun.set(false);
		}
	}

	public class ReadFuture implements Future<Record>, Comparable<ReadFuture> {
		private final long location;
		private Record r;
		private final AtomicBoolean isDone = new AtomicBoolean(false);

		public ReadFuture(long location) {
			this.location = location;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return false;
		}

		@Override
		public boolean isCancelled() {
			return false;
		}

		@Override
		public boolean isDone() {
			return isDone.get();
		}

		@Override
		public Record get() throws InterruptedException, ExecutionException {
			synchronized (isDone) {
				while (!isDone.get()) {
					isDone.wait();
				}
				if (isDone()) {
					return r;
				}
			}
			throw new ExecutionException(new RuntimeException("Did not complete the lookup"));
		}

		public void complete(Record r) {
			this.r = r;
			synchronized (isDone) {
				isDone.set(true);
				isDone.notifyAll();
			}
		}

		@Override
		public Record get(long timeout, TimeUnit unit)
				throws InterruptedException, ExecutionException, TimeoutException {
			return get();
		}

		public long getLocation() {
			return location;
		}

		@Override
		public int compareTo(ReadFuture o) {
			return this.location > o.location ? 1 : (this.location < o.location ? -1 : 0);
		}
	}
}
