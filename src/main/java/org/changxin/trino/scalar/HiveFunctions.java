package org.changxin.trino.scalar;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

/**
 * 开发标量函数（UDF）
 *
 */

public class HiveFunctions {

    @ScalarFunction("to_upper")//固定参数，表示函数名称
    @Description("Return default value if the value is NULL else return value")//函数注释
    @SqlType(StandardTypes.VARCHAR)//表示数据类型
    public static Slice toUpper(@SqlType(StandardTypes.VARCHAR) Slice input){

        //将获取到的数据转换为大写
        String s = input.toStringUtf8().toUpperCase();
        //将抓获后的数据放入内存返回
        return Slices.utf8Slice(s);
    }



}
