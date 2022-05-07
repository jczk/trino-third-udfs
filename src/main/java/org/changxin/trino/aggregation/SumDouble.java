package org.changxin.trino.aggregation;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.*;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.StandardTypes;
import org.changxin.trino.state.NullableDoubleState;

/**
 * 聚合函数（UDAF）
 *
 * https://trino.io/docs/current/develop/functions.html
 */

@AggregationFunction("sum_double")//函数名称
@Description("this is a double sum function")//函数注释
public class SumDouble {

    @InputFunction//输入方法注释
    public static void input(@AggregationState NullableDoubleState state,   //表示输入数据状态类型
                             @SqlType(StandardTypes.DOUBLE) double value    //输入类型
                            ){
        state.setNull(false);
        state.setDouble(state.getDouble() + value);
    }

    @CombineFunction//合并方法注释
    public static void combine(
            @AggregationState NullableDoubleState state, //中间聚合结果集
            @AggregationState NullableDoubleState otherState //表示每次进来的状态数据
            ){
        //判断第一条数据是否为空
        if (state.isNull()){
            state.setNull(false);
            state.setDouble(otherState.getDouble());
            return;
        }
        //如果有中间结果集的情况，需要累加
        state.setDouble(state.getDouble() + otherState.getDouble());
    }

    @OutputFunction(StandardTypes.DOUBLE)//输出方法注释
    public static void output(
            @AggregationState NullableDoubleState state, //聚合后的结果状态
            BlockBuilder out //设置返回结果
            ){
        //将结果以状态方式返回，因为每次获取的数据都是状态存储的，所以最后的结果也是以状态返回的
        NullableDoubleState.write(DoubleType.DOUBLE,state,out);
    }
}
