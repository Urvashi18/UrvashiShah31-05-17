package com.zycus.java.assignment.CustomDBMap;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DiskBackedMap<K extends Serializable, V extends Serializable>
		implements ConcurrentNavigableMap<K, V>, Closeable {
	private Logger log = Logger.getLogger(DiskBackedMap.class.getName());
	private Store<K, V> store;

	public DiskBackedMap(String dataDir) {
		this.store = new Store<K, V>(new Configuration().setDataDir(new File(dataDir)));
	}

	public DiskBackedMap(Configuration config) {
		this.store = new Store<K, V>(config);
	}

	@Override
	public void clear() {
		store.clear();
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean containsKey(Object key) {
		return store.get((K) key) != null;
	}

	@Override
	public boolean containsValue(Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return null;
	}

	@Override
	@SuppressWarnings("element-type-mismatch")
	public V get(Object key) {
		return store.get((K) key);
	}

	@Override
	public boolean isEmpty() {
		return store.size() == 0;
	}

	@Override
	public NavigableSet<K> keySet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public V put(K key, V value) {
		return store.save(key, value);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		for (K key : m.keySet()) {
			put(key, m.get(key));
		}
	}

	@Override
	public V remove(Object key) {
		V value = store.get((K) key);
		store.remove((K) key);
		return value;
	}

	@Override
	public int size() {
		return store.size();
	}

	@Override
	public Collection<V> values() {
		throw new UnsupportedOperationException();
	}

	public long sizeOnDisk() {
		return store.sizeOnDisk();
	}

	public void close() throws IOException {
		store.close();
	}

	public void gc() throws Exception {
		store.vacuum();
	}

	@Override
	public void finalize() throws Throwable {
		this.close();
		super.finalize();
	}

	public class Store<K extends Serializable, V extends Serializable> implements Closeable {
		private List<Page<K, V>> pages;
		private int magicNumber = 13;

		final protected ReadWriteLock lock = new ReentrantReadWriteLock();

		public Store(Configuration cfg) {
			init(cfg);
		}

		private void init(Configuration cfg) {
			pages = new ArrayList<Page<K, V>>(magicNumber);
			for (int i = 0; i < magicNumber; i++) {
				Configuration config = new Configuration(cfg);
				config.setNumber(i);
				pages.add(new Page<K, V>(config));
			}
		}

		/**
		 * Get the {@link ReadWriteLock} associated with this BTree. This should
		 * be used with browsing operations to ensure consistency.
		 *
		 * @return
		 */
		public ReadWriteLock getLock() {
			return lock;
		}

		public V save(K key, V value) {
			Page<K, V> kvPage = findPage(key);
			return kvPage.save(key, value);
		}

		public V get(K key) {
			return findPage(key).load(key);
		}

		private Page<K, V> findPage(K key) {
			int idx = key.hashCode() % magicNumber;
			return pages.get(Math.abs(idx));
		}

		private void remove(K key) {
			findPage(key).remove(key);
		}

		private int size() {
			int size = 0;
			for (Page<K, V> page : pages) {
				size = page.keyCount();
			}
			return size;
		}

		public synchronized void close() {
			for (Page page : pages) {
				page.close();
			}
		}

		public void vacuum() throws Exception {
			log.log(Level.INFO, "Starting gc process");
			long time = 0;
			for (Page<K, V> page : pages) {
				long pTime = System.currentTimeMillis();
				log.log(Level.INFO, "Started Vacuuming page:" + page.toString());
				page.vacuum();
				pTime = System.currentTimeMillis() - pTime;
				log.log(Level.INFO, "Completed Vacuuming page in :" + pTime + " ms");
				time += pTime;
			}
			log.log(Level.INFO, "Vacuum Complete:" + time + " ms");
		}

		public long sizeOnDisk() {
			long size = 0;
			for (Page<K, V> page : pages) {
				size = page.size();
			}
			return size;
		}

		public synchronized void clear() {
			for (Page<K, V> page : pages) {
				page.clear();
			}
		}
	}

	@Override
	public V putIfAbsent(K key, V value) {
		store.lock.writeLock().lock();
		try {
			if (!containsKey(key))
				return put(key, value);
			else
				return get(key);
		} finally {
			store.lock.writeLock().unlock();
		}
	}

	@Override
	public boolean remove(Object key, Object value) {
		store.lock.writeLock().lock();
		try {
			if (containsKey(key) && get(key).equals(value)) {
				remove(key);
				return true;
			} else
				return false;
		} finally {
			store.lock.writeLock().unlock();
		}
	}

	@Override
	public boolean replace(K key, V oldValue, V newValue) {
		store.lock.writeLock().lock();
		try {
			if (containsKey(key) && get(key).equals(oldValue)) {
				put(key, newValue);
				return true;
			} else
				return false;
		} finally {
			store.lock.writeLock().unlock();
		}
	}

	@Override
	public V replace(K key, V value) {
		store.lock.writeLock().lock();
		try {
			if (containsKey(key)) {
				return put(key, value);
			} else
				return null;
		} finally {
			store.lock.writeLock().unlock();
		}
	}

	@Override
	public java.util.Map.Entry<K, V> lowerEntry(K key) {
		throw new UnsupportedOperationException("not implemented yet");
	}

	@Override
	public K lowerKey(K key) {
		throw new UnsupportedOperationException("not implemented yet");
	}

	@Override
	public java.util.Map.Entry<K, V> floorEntry(K key) {
		throw new UnsupportedOperationException("not implemented yet");
	}

	@Override
	public K floorKey(K key) {
		throw new UnsupportedOperationException("not implemented yet");
	}

	@Override
	public java.util.Map.Entry<K, V> ceilingEntry(K key) {
		throw new UnsupportedOperationException("not implemented yet");
	}

	@Override
	public K ceilingKey(K key) {
		throw new UnsupportedOperationException("not implemented yet");
	}

	@Override
	public java.util.Map.Entry<K, V> higherEntry(K key) {
		throw new UnsupportedOperationException("not implemented yet");
	}

	@Override
	public K higherKey(K key) {
		throw new UnsupportedOperationException("not implemented yet");
	}

	@Override
	public java.util.Map.Entry<K, V> firstEntry() {
		throw new UnsupportedOperationException("not implemented yet");
	}

	@Override
	public java.util.Map.Entry<K, V> lastEntry() {
		throw new UnsupportedOperationException("not implemented yet");
	}

	@Override
	public java.util.Map.Entry<K, V> pollFirstEntry() {
		throw new UnsupportedOperationException("not implemented yet");
	}

	@Override
	public java.util.Map.Entry<K, V> pollLastEntry() {
		throw new UnsupportedOperationException("not implemented yet");
	}

	@Override
	public Comparator<? super K> comparator() {
		throw new UnsupportedOperationException("not implemented yet");
	}

	@Override
	public K firstKey() {
		throw new UnsupportedOperationException("not implemented yet");
	}

	@Override
	public K lastKey() {
		throw new UnsupportedOperationException("not implemented yet");
	}

	@Override
	public ConcurrentNavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
		throw new UnsupportedOperationException("not implemented yet");
	}

	@Override
	public ConcurrentNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
		throw new UnsupportedOperationException("not implemented yet");
	}

	@Override
	public ConcurrentNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
		throw new UnsupportedOperationException("not implemented yet");
	}

	@Override
	public ConcurrentNavigableMap<K, V> subMap(K fromKey, K toKey) {
		throw new UnsupportedOperationException("not implemented yet");
	}

	@Override
	public ConcurrentNavigableMap<K, V> headMap(K toKey) {
		throw new UnsupportedOperationException("not implemented yet");
	}

	@Override
	public ConcurrentNavigableMap<K, V> tailMap(K fromKey) {
		throw new UnsupportedOperationException("not implemented yet");
	}

	@Override
	public ConcurrentNavigableMap<K, V> descendingMap() {
		throw new UnsupportedOperationException("not implemented yet");
	}

	@Override
	public NavigableSet<K> navigableKeySet() {
		throw new UnsupportedOperationException("not implemented yet");
	}

	@Override
	public NavigableSet<K> descendingKeySet() {
		throw new UnsupportedOperationException("not implemented yet");
	}
}