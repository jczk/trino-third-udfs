 * 1、打包：mvn clean compile assembly:assembly
 * 2、将打好的jar包放到trino plugin目录下
 *  mkdir -p ${TRINO_HOME}/plugin/udf
 *  mv trino-third-udfs-jar-with-dependencies.jar ${TRINO_HOME}/plugin/udf
 * 3、重启trino：${TRINO_HOME}/bin/launcher restart
 * 4、trino --server localhost:8084 --catalog hive --schema ods
 * 5、执行查询
 *  show functions like 'to_upper';
 *  show functions like '%upper';
 *  select to_upper('abc');