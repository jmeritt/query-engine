package com.datadirect.util;

import org.jboss.logging.MDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teiid.core.util.StringUtil;
import org.teiid.logging.MessageLevel;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jmeritt on 3/18/15.
 */
public class SLFLogger implements org.teiid.logging.Logger {

    private ConcurrentHashMap<String, Logger> loggers = new ConcurrentHashMap<String, Logger>();

    @Override
    public boolean isEnabled(String context, int msgLevel) {
        Logger logger = getLogger(context);
        return willLog(logger, msgLevel);
    }

    private boolean willLog(Logger logger, int level) {
        switch (level) {
            case MessageLevel.CRITICAL:
            case MessageLevel.ERROR:
                return logger.isErrorEnabled();
            case MessageLevel.WARNING:
                return logger.isWarnEnabled();
            case MessageLevel.INFO:
                return logger.isInfoEnabled();
            case MessageLevel.DETAIL:
                return logger.isDebugEnabled();
            case MessageLevel.TRACE:
                return logger.isTraceEnabled();
        }
        return false;
    }

    private void logThrowable(Logger logger, int level, String msgs, Throwable t) {
        switch (level) {
            case MessageLevel.CRITICAL:
            case MessageLevel.ERROR:
                logger.error(msgs, t);
            case MessageLevel.WARNING:
                logger.warn(msgs, t);
            case MessageLevel.INFO:
                logger.info(msgs, t);
            case MessageLevel.DETAIL:
                logger.debug(msgs, t);
            case MessageLevel.TRACE:
                logger.trace(msgs, t);
        }

    }

    private void logMessage(Logger logger, int level, String msgs) {
        switch (level) {
            case MessageLevel.CRITICAL:
            case MessageLevel.ERROR:
                logger.error(msgs);
            case MessageLevel.WARNING:
                logger.warn(msgs);
            case MessageLevel.INFO:
                logger.info(msgs);
            case MessageLevel.DETAIL:
                logger.debug(msgs);
            case MessageLevel.TRACE:
                logger.trace(msgs);
        }

    }


    private Logger getLogger(String context) {
        Logger logger = loggers.get(context);
        if (logger == null) {
            logger = LoggerFactory.getLogger(context);
            loggers.put(context, logger);
        }
        return logger;
    }

    public void log(int level, String context, Object... msg) {
        Logger logger = getLogger(context);
        String msgStr = msg.length == 0 ? context : StringUtil.toString(msg, " ", false);
        logMessage(logger, level, msgStr);
    }

    public void log(int level, String context, Throwable t, Object... msg) {
        Logger logger = getLogger(context);
        String msgStr = msg.length == 0 ? context : StringUtil.toString(msg, " ", false);
        logThrowable(logger, level, msgStr, t);
    }


    public void shutdown() {
    }

    @Override
    public void putMdc(String key, String val) {
        MDC.put(key, val);
    }

    @Override
    public void removeMdc(String key) {
        MDC.remove(key);
    }

}
