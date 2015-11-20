package com.crassirostris.cache.refresh;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.interceptor.SimpleKey;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.context.ApplicationContext;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

import java.io.InvalidObjectException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * User: crassirostris
 * Date: 2015-10-14
 * Time: 오후 8:27
 */
@Slf4j
public class ClusterCache extends RefreshableCache {
	private List<String> cluster;
	private String thisCluster;
	private int randomKey;
	private RestTemplate restTemplate;

	@Autowired
	private ApplicationContext ac;

	public ClusterCache(String name, String... cluster) {
		super(name);
		setRestTemplate();
		Random random = new Random();
		randomKey = random.nextInt(Integer.MAX_VALUE);
		this.cluster = Lists.newArrayList(cluster);
		sortingCluster();
	}

	public void ping() {

		ArrayList<String> notEnableClusters = Lists.newArrayList();
		ArrayList<String> noticeThiscluster = Lists.newArrayList();
		for (String s : cluster) {
			pingCluster(s, s, notEnableClusters);
			if (StringUtils.isEmpty(thisCluster)) {
				noticeThiscluster.add(s);
			}
		}
		cluster.removeAll(notEnableClusters);
		sortingCluster();

		if (!StringUtils.isEmpty(thisCluster)) {
			for (String s : noticeThiscluster) {
				pingCluster(s, thisCluster, notEnableClusters);
			}
		}
		cluster.removeAll(notEnableClusters);
		sortingCluster();
	}

	private void pingCluster(String targetCluster, String localCluster,  List<String> notEnableClusters) {
		String format = "http://%s/clusterping/%s/%d?host=%s";
		try {
			restTemplate.getForObject(String.format(format, targetCluster, getName(), randomKey, localCluster), String.class);
		} catch (Exception e) {
			if (log.isDebugEnabled()) {
				log.warn(e.getMessage(), e);
			}
			notEnableClusters.add(targetCluster);
		}
	}

	public void receivePing(String hostAndPort, int key) {
		if (randomKey == key) {
			thisCluster = hostAndPort;
		}
		addCluster(hostAndPort);
	}

	private void setRestTemplate() {
		restTemplate = new RestTemplate();
		restTemplate.getMessageConverters().add(new MappingJackson2HttpMessageConverter());
		restTemplate.getMessageConverters().add(new StringHttpMessageConverter());
	}

	@Override
	public ValueWrapper get(Object key) {
		Class cls = RefreshableCacheHelper.getTargetMethodReturnClase(this.getName());
		Object o = get(key, cls);
		return (o != null ? new SimpleValueWrapper(fromStoreValue(o)) : null);
	}

	@Override
	public <T> T get(Object key, Class<T> type) {
		if (isThisCluster(key)) {
			T t = super.get(key, type);
			if (t == null) {
				Object result = RefreshableCacheHelper.excuteTargetMethod(ac, key, getName());
				Preconditions.checkArgument(result != null, String.format("key : %s is crash!! in %s", key, getName()));
				put(key, result);
			}
			return super.get(key, type);
		}

		int targetClusterIndex = getTargetClusterIndex(key);
		String targetCluster = cluster.get(targetClusterIndex);

		T data = null;
		try {
			data = getData(targetCluster, key, type);
			if (data == null) {
				throw new InvalidObjectException(targetCluster + " is not found data ("+key+") retry local cluster");
			}
		} catch (Exception e) {
			log.warn(e.getMessage(), e);
			cluster.remove(targetClusterIndex);
			sortingCluster();
			return super.get(key, type);
		}

		evict(key); // delete local data
		if (log.isDebugEnabled()) {
			log.debug("get data from " + targetCluster + " and local key evict:" + key );
		}
		return data;
	}

	private <T> T getData(String targetCluster, Object key, Class<T> type) {
		String url = getUrl(targetCluster, key);
		T forObject = restTemplate.getForObject(url, type);
		return forObject;
	}

	private void sortingCluster() {
		Collections.sort(cluster);
	}

	private String getUrl(String targetCluster, Object key) {
		if (key instanceof SimpleKey) {
			SimpleKey key1 = (SimpleKey) key;
			key = key1.toString();
		}
		return String.format("http://%s%s/%s/%s", targetCluster, ClusterCacheAdapter.REQUEST_URI, this.getName(), key);
	}

	private boolean isThisCluster(Object key) {
		if (StringUtils.isEmpty(thisCluster)) {
			ping();
		}
		int targetClusterIndex = getTargetClusterIndex(key);
		int thisClusterIndex = getThisClusterIndex();
		return targetClusterIndex == thisClusterIndex;
	}

	private int getThisClusterIndex() {
		if (!StringUtils.isEmpty(thisCluster)) {
			return cluster.indexOf(thisCluster);
		}
		throw new IllegalStateException("ClusterCache: Invalid setting Cluster properties!");
	}

	private int getTargetClusterIndex(Object key) {
		String s = key.toString();
		String substring = s.substring(s.length() - 1, s.length());
		byte[] bytes = substring.getBytes(Charset.forName("UTF-8"));
		int targetClusterIndex = bytes[0] % cluster.size();
		return targetClusterIndex;
	}

	public void addCluster(String s) {
		if (cluster.contains(s)) {
			return;
		}
		cluster.add(s);
		sortingCluster();
	}
}
