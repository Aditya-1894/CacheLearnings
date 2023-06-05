package com.oxane.argon.core.caching;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.cache.Cache.Entry;
import javax.validation.constraints.NotNull;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.transactions.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.data.jpa.repository.JpaRepository;

import com.oxane.argon.core.errors.DataException;

/**
 * @param <K>  the Data Type of Id Column
 * @param <V>  Current class 
 */
public class OxaneCache<K, V extends Cacheable<K, V>> {

	IgniteCache<K, V> cache;

	JpaRepository<V, K> jpaRepository;

	CacheConfiguration<K, V> cacheCfg;

	Ignite ignite;

	public static IgniteTransactions transactions;

	private final Logger logger = LoggerFactory.getLogger(OxaneCache.class);

	public OxaneCache(Ignite ignite, IgniteCache<K, V> cache, JpaRepository<V, K> jpaRepository, CacheConfiguration<K, V> cacheCfg) {
		this.ignite = ignite;
		this.cache = cache;
		this.jpaRepository = jpaRepository;
		this.cacheCfg = cacheCfg;

		if (transactions == null) {
			transactions = ignite.transactions();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected void setIgniteCache(IgniteCache cache) {
		this.cache = cache;
	}

	protected CacheConfiguration<K, V> getCacheConfiguration() {
		return cacheCfg;
	}

	public V get(K id) {
		V val = cache.get(id);
		if (val == null) {
			Optional<V> optional = jpaRepository.findById(id);
			if (optional.isPresent()) {
				val = optional.get();
				cache.put(id, val.copy());
			}
		}
		return val;
	}
	
	public V reload(K id) {
		V val = null;
		Optional<V> optional = jpaRepository.findById(id);
		if (optional.isPresent()) {
			val = optional.get();
			cache.put(id, val.copy());
		}
		return val;
	}

	public V getReferenceById(K id) {
		return get(id);
	}

	public Optional<V> findById(K id) {
		Optional<V> optional = Optional.ofNullable(cache.get(id));
		if (optional.isEmpty()) {
			optional = jpaRepository.findById(id);
			if (optional.isPresent())
				cache.put(id, optional.get().copy());
		}
		return optional;
	}

	public boolean existsById(K key) {
		if (cache.containsKey(key))
			return true;
		return jpaRepository.existsById(key);
	}

	public V save(V val) {
		val = jpaRepository.save(val);
		cache.put(val.getId(), val.copy());
		return val;
	}

	public void saveAll(Collection<V> values) {
		try (Transaction tx = transactions.txStart()) {
			jpaRepository.saveAll(values);
			for (V val : values) {
				cache.put(val.getId(), val.copy());
			}
			tx.commit();
		}
	}
	
	public V saveInCache(V val) {
		cache.put(val.getId(), val.copy());
		return val;
	}

	public void removeAllFromCache(Set<K> ids) {
		cache.removeAll(ids);
	}

	public void removeFromCache(K id) {
		cache.remove(id);
	}

	public void delete(V val) {
		jpaRepository.delete(val);
		cache.remove(val.getId());
	}

	public void delete(K id) {
		jpaRepository.deleteById(id);
		cache.remove(id);
	}

	/**
	 * @param <V>  List of entities to delete
	 * @param <K>  Set of ids of entities to delete
	 */
	public void deleteAll(List<V> data, Set<K> ids) {
		jpaRepository.deleteAllInBatch(data);
		cache.removeAll(ids);
	}

	public void deleteById(K id) {
		delete(id);
	}

	public void deleteAll(BiFunction<K, V, Boolean> function) {
		ScanQuery<K, V> scanQuery = new ScanQuery<>((k, v) -> function.apply(k, v));
		try (org.apache.ignite.cache.query.QueryCursor<Entry<K, V>> cursor = cache.query(scanQuery)) {
			for (Entry<K, V> entry : cursor) {
				delete(entry.getValue());
			}
		}
	}

	public void loadAll(Set<V> data) {
		cache.clear();
		cache.putAll(data.stream().collect(Collectors.toMap(Cacheable::getId, val -> val.copy())));
	}

	public void loadAll() {
		jpaRepository.flush();
		List<V> all = jpaRepository.findAll();
		cache.clear();
		cache.putAll(all.stream().collect(Collectors.toMap(Cacheable::getId, val -> val.copy())));
	}

	protected IgniteFuture<Long> size() {
		return cache.sizeLongAsync(CachePeekMode.ALL);
	}

	public void popluateCache() {
		if (size().get() == 0) {
			if (jpaRepository instanceof CacheableRepository) {
				loadAll(((CacheableRepository<V, K>) jpaRepository).findAllForCache());
			} else {
				loadAll();
			}
		}
	}

	public List<V> findAll(@NotNull Set<K> keys) {
		return new ArrayList<>(cache.getAll(keys).values());
	}

	/*public <T> List<T> findAll(@NotNull Set<K> keys, @NotNull Class<T> dtoClazz) {
		IgniteCache<K, BinaryObject> binaryCache = cache.withKeepBinary();
	
		List<T> list = new ArrayList<>();
	
		Field[] fields = dtoClazz.getDeclaredFields();
		binaryCache.getAll(keys).forEach((key, val) -> {
			list.add(transformClass(val, dtoClazz, fields));
		});
		return list;
	}*/

	/**
	 * All fields in Pojo should be marked with Annotation {@link QuerySqlField}
	 * @param keys
	 * @return
	 */
	/*public List<Map<String, Object>> findAll(String... keys) {
		StringBuilder sb = new StringBuilder(512);
		sb.append("select ").append(Arrays.stream(keys).collect(Collectors.joining(","))).append(" from ").append(cache.getName());
		List<Map<String, Object>> list = new ArrayList<>();
		cache.query(new SqlFieldsQuery(sb.toString())).forEach(resultList -> {
			Map<String, Object> map = new HashMap<>(keys.length + 1, 1);
			for (int i = 0; i < resultList.size(); i++) {
				map.put(keys[i], resultList.get(i));
			}
			list.add(map);
		});
	
		return list;
	}*/

	public <T> List<T> findAll(@NotNull Class<T> clazz) {
		IgniteCache<K, BinaryObject> binaryCache = cache.withKeepBinary();

		List<T> list = new ArrayList<>();

		Field[] fields = clazz.getDeclaredFields();
		binaryCache.forEach(action -> {
			list.add(transformClass(action.getValue(), clazz, fields));
		});
		return list;
	}

	public <T> List<T> findAll(@NotNull Class<T> clazz, Function<V, Boolean> function) {
		ScanQuery<K, V> scanQuery = new ScanQuery<>((k, v) -> function.apply(v));
		return findAll(clazz, scanQuery);
	}
	
	public <T> List<T> findAll(@NotNull Class<T> clazz, BiFunction<K, V, Boolean> function) {
		ScanQuery<K, V> scanQuery = new ScanQuery<>((k, v) -> function.apply(k, v));
		return findAll(clazz, scanQuery);
	}
	
	private <T> List<T> findAll(@NotNull Class<T> clazz, ScanQuery<K, V> scanQuery) {
		List<T> list = new ArrayList<>();
		try (org.apache.ignite.cache.query.QueryCursor<Entry<K, V>> cursor = cache.query(scanQuery)) {
			for (Entry<K, V> entry : cursor) {
				T object = null;
				try {
					object = clazz.getDeclaredConstructor().newInstance();
					BeanUtils.copyProperties(entry.getValue(), object);
					list.add(object);
				} catch (Exception e) {
					logger.error(e.getMessage());
					throw new DataException(clazz.getName() + "fields not matching with " + cacheCfg.clazz.getName() + " fields.");
				}
			}
		}
		return list;
	}

	private <T> T transformClass(BinaryObject binaryObject, Class<T> clazz, Field[] fields) {
		T object = null;
		try {
			object = clazz.getDeclaredConstructor().newInstance();
			for (Field field : fields) {
				if (java.lang.reflect.Modifier.isStatic(field.getModifiers()) || java.lang.reflect.Modifier.isTransient(field.getModifiers())) {
					continue;
				}
				if (!field.canAccess(object))
					field.setAccessible(true);
				Object fieldObject = binaryObject.field(field.getName());
				if (fieldObject != null) {
					if (field.getType().toString().contains("com.oxane")) {
						Class<?> clazz1 = field.getType();
						field.set(object, transformClass((BinaryObject) binaryObject.field(field.getName()), clazz1, clazz1.getDeclaredFields()));
					} else {
						field.set(object, binaryObject.field(field.getName()));
					}
				}
			}
			return object;
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		return object;
	}

	private List<V> findAll(ScanQuery<K, V> scanQuery) {
		List<V> list = new ArrayList<>();
		try (org.apache.ignite.cache.query.QueryCursor<Entry<K, V>> cursor = cache.query(scanQuery)) {
			cursor.forEach(action -> list.add(action.getValue()));
		}
		return list;
	}
	
	public List<V> findAll(BiFunction<K, V, Boolean> function) {
		ScanQuery<K, V> scanQuery = new ScanQuery<>((k, v) -> function.apply(k, v));
		return findAll(scanQuery);
	}

	public V findOne(BiFunction<K, V, Boolean> function) {
		ScanQuery<K, V> scanQuery = new ScanQuery<>((k, v) -> function.apply(k, v));
		List<V> list = findAll(scanQuery);
		if (list.isEmpty()) {
			return null;
		} else if (list.size() > 1) {
			throw new DataException("No Unique Result");
		}
		return list.get(0);
	}

	/*public List<V> findAll(SqlQuery<K, V> sqlQuery) {
		List<V> list = new ArrayList<>();
		try (org.apache.ignite.cache.query.QueryCursor<Entry<K, V>> cursor = cache.query(sqlQuery)) {
			cursor.forEach(action -> list.add(action.getValue()));
		}
		return list;
	}*/

	/*public List<V> findAll(int transactionId) {
		@SuppressWarnings("rawtypes")
		ScanQuery<K, V> scanQuery = new ScanQuery<>(
				(K, transactionDependent) -> ((TransactionCacheable) transactionDependent).getTransactionId() == transactionId);
		List<V> all = new ArrayList<>();
		try (org.apache.ignite.cache.query.QueryCursor<Entry<K, V>> cursor = cache.query(scanQuery)) {
			cursor.forEach(action -> all.add(action.getValue()));
		}
		return all;
	}*/

	public List<V> findAll() {
		List<V> all = new ArrayList<>();
		cache.forEach(action -> {
			all.add(action.getValue());
		});
		return all;
	}

	public boolean isEmpty() {
		return cache.size(CachePeekMode.ALL) == 0;
	}

	/**
	 * Queries cache. Accepts any subclass of {@link Query} interface.
	 * See also {@link #query(SqlFieldsQuery)}.
	 */
	/*public <R> QueryCursor<R> query(Query<R> qry) {
		return (QueryCursor<R>) cache.query(qry);
	}*/

	/**
	 * Queries cache. Accepts {@link SqlFieldsQuery} class.
	 *
	 * @param qry SqlFieldsQuery.
	 * @return Cursor.
	 * @see SqlFieldsQuery
	 */
	/*public FieldsQueryCursor<List<?>> query(SqlFieldsQuery qry) {
		return (FieldsQueryCursor<List<?>>) cache.query(qry);
	}*/
}
