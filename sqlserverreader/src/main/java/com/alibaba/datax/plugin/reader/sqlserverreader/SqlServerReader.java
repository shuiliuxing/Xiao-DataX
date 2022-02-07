package com.alibaba.datax.plugin.reader.sqlserverreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class SqlServerReader extends Reader {
    private static final DataBaseType DATABASE_TYPE=DataBaseType.SQLServer;

    // Job实现
    public static class Job extends Reader.Job {
        private static final Logger LOG= LoggerFactory.getLogger(Job.class);
        private Configuration originalConfig=null;
        private CommonRdbmsReader.Job commonRdbmsReaderJob;

        @Override
        public void init(){
            this.originalConfig=super.getPluginJobConf();
            Integer fetchSize=this.originalConfig.getInt(com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE,
                    Constant.DEFAULT_FETCH_SIZE);
            if(fetchSize<1){
                throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE,
                        String.format("您配置的fetchSize有误，根据DataX的设计，fetchSize : [%d] 设置值不能小于 1.", fetchSize));
            }
            this.originalConfig.set(com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE,fetchSize);

            this.commonRdbmsReaderJob=new CommonRdbmsReader.Job(DATABASE_TYPE);
            this.commonRdbmsReaderJob.init(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int acviceNumber){
            return this.commonRdbmsReaderJob.split(this.originalConfig, acviceNumber);
        }

        @Override
        public void post(){
            this.commonRdbmsReaderJob.post(this.originalConfig);
        }

        @Override
        public void destroy(){
            this.commonRdbmsReaderJob.destroy(this.originalConfig);
        }
    }

    // Task的实现
    public static class Task extends Reader.Task{
        private Configuration readerSliceconfig;
        private CommonRdbmsReader.Task commonRdbmsReaderTask;

        @Override
        public void init(){
            this.readerSliceconfig=super.getPluginJobConf();
            this.commonRdbmsReaderTask=new CommonRdbmsReader.Task(DATABASE_TYPE, super.getTaskGroupId(), super.getTaskId());
            this.commonRdbmsReaderTask.init(this.readerSliceconfig);
        }

        @Override
        public void startRead(RecordSender recordSender){
            int fetchSize=this.readerSliceconfig.getInt(com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE);
            this.commonRdbmsReaderTask.startRead(this.readerSliceconfig, recordSender, super.getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void post(){
            this.commonRdbmsReaderTask.post(this.readerSliceconfig);
        }

        @Override
        public void destroy(){
            this.commonRdbmsReaderTask.destroy(this.readerSliceconfig);
        }
    }
}
