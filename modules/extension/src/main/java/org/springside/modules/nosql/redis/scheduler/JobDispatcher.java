package org.springside.modules.nosql.redis.scheduler;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springside.modules.nosql.redis.JedisScriptExecutor;
import org.springside.modules.nosql.redis.JedisTemplate;
import org.springside.modules.utils.Threads;
import org.springside.modules.utils.Threads.WrapExceptionRunnable;

import redis.clients.jedis.JedisPool;

import com.google.common.collect.Lists;

/**
 * 定时分发任务。 启动线程定时从scheduled job sorted set 中取出到期的任务放入ready job list.
 * 线程池可自行创建，也可以从外部传入共用。
 * 
 * @author calvin
 */
public class JobDispatcher implements Runnable {
	public static final String DEFAULT_DISPATCH_LUA_FILE = "classpath:/redis/dispatch.lua";
	public static final long DEFAULT_INTERVAL_MILLIS = 1000;
	public static final boolean DEFAULT_RELIABLE = false;
	public static final long DEFAULT_TIMEOUT_SECONDS = 60;

	private static Logger logger = LoggerFactory.getLogger(JobDispatcher.class);

	private static AtomicInteger poolNumber = new AtomicInteger(1);
	private ScheduledExecutorService internalScheduledThreadPool;

	private ScheduledFuture dispatchJob;
	private long intervalMillis = DEFAULT_INTERVAL_MILLIS;
	private boolean reliable = DEFAULT_RELIABLE;
	private long timeoutSecs = DEFAULT_TIMEOUT_SECONDS;

	private JedisTemplate jedisTemplate;
	private JedisScriptExecutor scriptExecutor;
	private String scriptPath = DEFAULT_DISPATCH_LUA_FILE;

	private List<String> keys;
	private String scheduledJobKey;
	private String readyJobKey;
	private String lockJobKey;
	private String dispatchCounterKey;
	private String retryCounterKey;

	public JobDispatcher(String jobName, JedisPool jedisPool) {
		scheduledJobKey = Keys.getScheduledJobKey(jobName);
		readyJobKey = Keys.getReadyJobKey(jobName);
		dispatchCounterKey = Keys.getDispatchCounterKey(jobName);
		lockJobKey = Keys.getLockJobKey(jobName);
		retryCounterKey = Keys.getRetryCounterKey(jobName);

		keys = Lists.newArrayList(scheduledJobKey, readyJobKey, dispatchCounterKey, lockJobKey, retryCounterKey);

		jedisTemplate = new JedisTemplate(jedisPool);
		scriptExecutor = new JedisScriptExecutor(jedisPool);
	}

	/**
	 * 启动分发线程, 自行创建scheduler线程池.
	 */
	public void start() {
		internalScheduledThreadPool = Executors.newScheduledThreadPool(1,
				Threads.buildJobFactory("Job-Dispatcher-" + poolNumber.getAndIncrement() + "-%d"));

		start(internalScheduledThreadPool);
	}

	/**
	 * 启动分发线程, 使用传入的scheduler线程池.
	 */
	public void start(ScheduledExecutorService scheduledThreadPool) {
		scriptExecutor.loadFromFile(scriptPath);

		dispatchJob = scheduledThreadPool.scheduleAtFixedRate(new WrapExceptionRunnable(this), 0, intervalMillis,
				TimeUnit.MILLISECONDS);
	}

	/**
	 * 停止分发任务，如果是自行创建的threadPool则自行销毁，关闭时最多等待5秒。
	 */
	public void stop() {
		dispatchJob.cancel(false);

		if (internalScheduledThreadPool != null) {
			Threads.normalShutdown(internalScheduledThreadPool, 5, TimeUnit.SECONDS);
		}
	}

	/**
	 * 以当前时间为参数执行Lua Script分发任务。
	 */
	@Override
	public void run() {
		long currTime = System.currentTimeMillis();
		List<String> args = Lists.newArrayList(String.valueOf(currTime), String.valueOf(reliable),
				String.valueOf(timeoutSecs));
		scriptExecutor.execute(keys, args);
	}

	/**
	 * 获取未达到触发条件进行分发的Job数量.
	 */
	public long getScheduledJobNumber() {
		return jedisTemplate.zcard(scheduledJobKey);
	}

	/**
	 * 获取已分发但未被执行的Job数量.
	 */
	public long getReadyJobNumber() {
		return jedisTemplate.llen(readyJobKey);
	}

	/**
	 * 获取reliable job时已被取走执行但未报告完成的Job数量.
	 */
	public long getLockJobNumber() {
		return jedisTemplate.zcard(lockJobKey);
	}

	/**
	 * 获取已分发的Job数量。
	 */
	public long getDispatchCounter() {
		return jedisTemplate.getAsLong(dispatchCounterKey);
	}

	/**
	 * 获取已重做的Job数量。
	 */
	public long getRetryCounter() {
		return jedisTemplate.getAsLong(retryCounterKey);
	}

	/**
	 * 重置已分发的Job数量计数器.
	 */
	public void restCounter() {
		jedisTemplate.set(dispatchCounterKey, "0");
	}

	/**
	 * 设置非默认的script path, 格式为spring的Resource路径风格。
	 */
	public void setScriptPath(String scriptPath) {
		this.scriptPath = scriptPath;
	}

	/**
	 * 设置非默认1秒的分发间隔.
	 */
	public void setIntervalMillis(long intervalMillis) {
		this.intervalMillis = intervalMillis;
	}

	public void setReliable(boolean reliable) {
		this.reliable = reliable;
	}

	public void setTimeoutSecs(long timeoutSecs) {
		this.timeoutSecs = timeoutSecs;
	}
}
