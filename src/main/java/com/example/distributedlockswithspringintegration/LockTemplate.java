package com.example.distributedlockswithspringintegration;

import org.springframework.integration.support.locks.LockRegistry;
import org.springframework.util.ReflectionUtils;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public class LockTemplate {

	private final LockRegistry registry;

	public LockTemplate(LockRegistry registry) {
		this.registry = registry;
	}

	public <T> T executeWithLock(String key, int timeoutDuration, TimeUnit tu, Callable<T> callable) {
		return this.doExecuteWithLock(key, lock -> lock.tryLock(timeoutDuration, tu), callable);
	}

	public <T> T executeWithLock(String key, Callable<T> callable) {
		return this.doExecuteWithLock(key, Lock::tryLock, callable);
	}

	private <T> T doExecuteWithLock(
		String key, ExceptionSwallowingFunction<Lock, Boolean> lockProducer, Callable<T> callable) {

		try {
			Lock lock = registry.obtain(key);
			boolean lockAcquired = lockProducer.apply(lock);
			if (lockAcquired) {
				try {
					return callable.call();
				}
				finally {
					lock.unlock();
				}
			}
		}
		catch (Exception e) {
			ReflectionUtils.rethrowRuntimeException(e);
		}
		return null;
	}

	private interface ExceptionSwallowingFunction<I, O> {
		O apply(I i) throws Exception;
	}
}
