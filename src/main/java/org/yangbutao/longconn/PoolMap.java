package org.yangbutao.longconn;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * map key到集合Pool，这里的key是connectionId,集合Pool中的每一个元素表示一个connection对象
 * 
 * @author yangbutao
 * 
 * @param <K>
 * @param <V>
 */
public class PoolMap<K, V> implements Map<K, V> {
	private PoolType poolType;

	private int poolMaxSize;

	private Map<K, Pool<V>> pools = new ConcurrentHashMap<K, Pool<V>>();

	public PoolMap(PoolType poolType) {
		this.poolType = poolType;
	}

	public PoolMap(PoolType poolType, int poolMaxSize) {
		this.poolType = poolType;
		this.poolMaxSize = poolMaxSize;
	}

	public V get(Object key) {
		Pool<V> pool = pools.get(key);
		return pool != null ? pool.get() : null;
	}

	public V put(K key, V value) {
		Pool<V> pool = pools.get(key);
		if (pool == null) {
			pools.put(key, pool = createPool());
		}
		return pool != null ? pool.put(value) : null;
	}

	@SuppressWarnings("unchecked")
	public V remove(Object key) {
		Pool<V> pool = pools.remove(key);
		if (pool != null) {
			remove((K) key, pool.get());
		}
		return null;
	}

	public boolean remove(K key, V value) {
		Pool<V> pool = pools.get(key);
		boolean res = false;
		if (pool != null) {
			res = pool.remove(value);
			if (res && pool.size() == 0) {
				pools.remove(key);
			}
		}
		return res;
	}

	public Collection<V> values() {
		Collection<V> values = new ArrayList<V>();
		for (Pool<V> pool : pools.values()) {
			Collection<V> poolValues = pool.values();
			if (poolValues != null) {
				values.addAll(poolValues);
			}
		}
		return values;
	}

	public Collection<V> values(K key) {
		Collection<V> values = new ArrayList<V>();
		Pool<V> pool = pools.get(key);
		if (pool != null) {
			Collection<V> poolValues = pool.values();
			if (poolValues != null) {
				values.addAll(poolValues);
			}
		}
		return values;
	}

	public boolean isEmpty() {
		return pools.isEmpty();
	}

	public int size() {
		return pools.size();
	}

	public int size(K key) {
		Pool<V> pool = pools.get(key);
		return pool != null ? pool.size() : 0;
	}

	public boolean containsKey(Object key) {
		return pools.containsKey(key);
	}

	public boolean containsValue(Object value) {
		if (value == null) {
			return false;
		}
		for (Pool<V> pool : pools.values()) {
			if (value.equals(pool.get())) {
				return true;
			}
		}
		return false;
	}

	public void putAll(Map<? extends K, ? extends V> map) {
		for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
			put(entry.getKey(), entry.getValue());
		}
	}

	public void clear() {
		for (Pool<V> pool : pools.values()) {
			pool.clear();
		}
		pools.clear();
	}

	public Set<K> keySet() {
		return pools.keySet();
	}

	public Set<Map.Entry<K, V>> entrySet() {
		Set<Map.Entry<K, V>> entries = new HashSet<Entry<K, V>>();
		for (Map.Entry<K, Pool<V>> poolEntry : pools.entrySet()) {
			final K poolKey = poolEntry.getKey();
			final Pool<V> pool = poolEntry.getValue();
			for (final V poolValue : pool.values()) {
				if (pool != null) {
					entries.add(new Map.Entry<K, V>() {

						public K getKey() {
							return poolKey;
						}

						public V getValue() {
							return poolValue;
						}

						public V setValue(V value) {
							return pool.put(value);
						}
					});
				}
			}
		}
		return null;
	}

	protected interface Pool<R> {
		public R get();

		public R put(R resource);

		public boolean remove(R resource);

		public void clear();

		public Collection<R> values();

		public int size();
	}

	public enum PoolType {
		RoundRobin;

		public static PoolType valueOf(String poolTypeName,
				PoolType defaultPoolType, PoolType... allowedPoolTypes) {
			PoolType poolType = PoolType.fuzzyMatch(poolTypeName);
			if (poolType != null) {
				boolean allowedType = false;
				if (poolType.equals(defaultPoolType)) {
					allowedType = true;
				} else {
					if (allowedPoolTypes != null) {
						for (PoolType allowedPoolType : allowedPoolTypes) {
							if (poolType.equals(allowedPoolType)) {
								allowedType = true;
								break;
							}
						}
					}
				}
				if (!allowedType) {
					poolType = null;
				}
			}
			return (poolType != null) ? poolType : defaultPoolType;
		}

		public static String fuzzyNormalize(String name) {
			return name != null ? name.replaceAll("-", "").trim().toLowerCase()
					: "";
		}

		public static PoolType fuzzyMatch(String name) {
			for (PoolType poolType : values()) {
				if (fuzzyNormalize(name)
						.equals(fuzzyNormalize(poolType.name()))) {
					return poolType;
				}
			}
			return null;
		}
	}

	protected Pool<V> createPool() {

		return new RoundRobinPool<V>(poolMaxSize);
	}

	/**
	 * 实现RoundRobin算法的Pool，每次取connection时是按照RR算法取的
	 * @author yangbutao
	 *
	 * @param <R>
	 */
	@SuppressWarnings("serial")
	class RoundRobinPool<R> extends CopyOnWriteArrayList<R> implements Pool<R> {
		private int maxSize;
		private int nextResource = 0;

		public RoundRobinPool(int maxSize) {
			this.maxSize = maxSize;
		}

		public R put(R resource) {
			if (size() < maxSize) {
				add(resource);
			}
			return null;
		}

		public R get() {
			//
			if (size() < maxSize) {
				return null;
			}
			nextResource %= size();
			R resource = get(nextResource++);
			return resource;
		}

		public Collection<R> values() {
			return this;
		}

	}
}
