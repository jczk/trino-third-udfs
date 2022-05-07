package org.changxin.trino.funnel;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.*;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.StandardTypes;
import org.changxin.trino.state.SliceState;

/**
 * 漏斗深度聚合
 *
 * 第一个参数是漏斗深度，第二个参数是事件个数
 * funnel_merge(user_dept,4)
 */

@AggregationFunction("funnel_merge")
public class FunnelMerge {

    @InputFunction
    public static void input(SliceState state,
                             @SqlType(StandardTypes.INTEGER) long userState,
                             @SqlType(StandardTypes.INTEGER) long eventCount){

        //获取状态
        Slice slice = state.getSlice();
        //判断状态是否为空
        if (null == slice){
            //表示长度空间为16，也就是每个事件下面的用户能存入的数量在Int类型以内
            //为了保证长度够用，即用户能放得下选择Int，没有选择byte
            slice = Slices.allocate((int)eventCount * 4);
        }

        //计算每个事件下的用户数，按照用户的深度计算相应位置的值
        //比如用户深度为2，那么就是[1,1,0,0] 表示前两位发生了该事件
        for (int status = 0 ;status <userState;++status){
            int index = status * 4;
            slice.setInt(index,slice.getInt(index) + 1);
        }
        //将结果返回
        state.setSlice(slice);
    }

    @CombineFunction
    public static void combine(SliceState state,SliceState otherState){
        //获取状态数据
        Slice slice = state.getSlice();
        Slice otherSlice = otherState.getSlice();

        //判断是否为空
        if (null == slice){
            //如果第一次执行为空，直接赋值otherSlice
            state.setSlice(otherSlice);
        }else{
            //循环累加两个状态数据结果
            for (int index = 0; index < slice.length();index += 4){
                slice.setInt(index,slice.getInt(index) + otherSlice.getInt(index));
            }
            //返回聚合结果
            state.setSlice(slice);
        }
    }


    @OutputFunction("array<bigint>")
    public static void output(SliceState state, BlockBuilder out){
        //获取状态
        Slice slice = state.getSlice();
        //判断是否为空
        if (null == slice){
            out.closeEntry();
            return;
        }
        //如果不为空，把数据加载到返回对象中
        //最终结果：[8000,6666,5555,2000]
        for (int index =0;index < slice.length(); index += 4){
            //返回的是数组结构，相应循环取值，因为每一个用户数在对应的位置
            BigintType.BIGINT.writeLong(out.beginBlockEntry(),slice.getInt(index));
        }

        out.closeEntry();
    }
}
