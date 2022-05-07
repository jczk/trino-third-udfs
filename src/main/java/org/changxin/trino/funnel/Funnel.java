package org.changxin.trino.funnel;


import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.*;
import io.trino.spi.type.StandardTypes;
import org.changxin.trino.state.SliceState;

import java.util.*;

/**
 * 计算漏斗深度（用户深度）
 * funnel(ctime,86400*1000*3,event,'SingnUp,AppPageView,AppClick,NewsAction')
 *
 */

@AggregationFunction("funnel")//函数名称
public class Funnel extends Base{

    //状态的常量，表示要放两个Int [窗口大小] [事件个数]
    private static final int COUNT_FLAG_LENGTH = 8;

    //设置每个事件所占的位数，这个常量表示要存入时间戳和事件下标
    private static final int COUNT_ONE_LENGTH = 5;

    //处理输入的数据
    @InputFunction
    public static void input(SliceState state,//保存数据的状态
                             @SqlType(StandardTypes.BIGINT) long eventTime, //事件发生时间
                             @SqlType(StandardTypes.BIGINT) long windows, //窗口期
                             @SqlType(StandardTypes.VARCHAR)Slice event, //事件名称
                             @SqlType(StandardTypes.VARCHAR) Slice events //所有事件，每个事件以逗号分隔
                             ){
        //获取状态
        Slice slice = state.getSlice();
        //判断状态是否有初始化
        if (!event_pos_dict.containsKey(events)){//表示要初始化
            initEvents(events);
        }
        //首先判断Slice状态是否拿到数据值
        if (null == slice){
            //如果这个空间没有被分配，需要分配空间
            //设计的初衷为：窗口大小[4byte]，事件个数[4byte]，事件时间[4byte]，事件索引[1byte]
            //前两个是设计的常量，后面五个也是常量，是事件时间和索引

            //开辟空间区域
            slice = Slices.allocate(COUNT_FLAG_LENGTH + COUNT_ONE_LENGTH);
            //将内容赋值-窗口期
            slice.setInt(0,(int)windows);
            //事件个数，通过Base里面获取个数传入
            slice.setInt(4,event_pos_dict.get(events).size());
            /**
             * 保存事件发生的时间和事件对应的索引号
             * 注释：
             * 这里可以对这个事件时间进行操作，可以减去一个固定值，然后让其小于int类型的长度即可，
             * 或者最坏的打算存Long，但这样不好，因为开销会增大一倍，不可取，尽量不要改类型空间大小。
             */
            slice.setInt(COUNT_FLAG_LENGTH,(int)eventTime);
            //事件对应的索引
            slice.setByte(COUNT_FLAG_LENGTH+4,event_pos_dict.get(events).get(event));
            //将所有字节数据保存到状态中，并返回结果
            state.setSlice(slice);
        }
        //如果之前数据不为空，就追加数据
        else{
            //获取上一个slice的长度
            int slice_length = slice.length();
            //新建slice，并初始化，在上一个长度后面追加新的数据
            Slice newSlice = Slices.allocate(slice_length + COUNT_ONE_LENGTH);
            //这里不需要追加前面的窗口大小和事件个数，因为是固定的，不需要重复添加
            //将上一次有的数据重新添加进去
            newSlice.setBytes(0,slice.getBytes());
            //然后追加新的事件时间和索引
            newSlice.setInt(slice_length,(int)eventTime);
            newSlice.setByte(slice_length+4,event_pos_dict.get(events).get(event));
            //将所有数据返回到状态中，数据格式：
            // [窗口大小[4]，事件个数[4]，事件时间[4]，事件索引[1],窗口大小[4]，事件个数[4]，事件时间[4]，事件索引[1],...]
            state.setSlice(newSlice);
        }

    }

    @CombineFunction
    public static void combine(SliceState state, //聚合结果集
                               SliceState otherState //每次新的状态数据
     ){
        //获取状态
        Slice slice = state.getSlice();
        //另一个状态
        Slice otherSlice = otherState.getSlice();
        //将两个状态组合成一个状态
        if (null == slice){
            //判断为空，第一次进入，将otherState赋值给state
            otherState.setSlice(otherSlice);
        } else{
            //如果不是空，那么就先把两个长度获取出来
            int slice_len = slice.length();
            int other_slice_len = otherSlice.length();

            //将两个长度加在一起，再拼接前面原有的8个字节，因为前面有两个常量
            Slice newSlice = Slices.allocate(slice_len + other_slice_len - COUNT_FLAG_LENGTH);

            //将开辟的内存区域赋值
            //[窗口大小[4]，事件个数[4]，事件时间[4]，事件索引[1]
            newSlice.setBytes(0,slice.getBytes());

            //下一个状态赋值之前要把前面8位去掉，因为不需要重复
            //截取长度数据：[事件时间[4]，事件索引[1]]
            newSlice.setBytes(slice_len,slice.getBytes(),COUNT_FLAG_LENGTH,other_slice_len - COUNT_FLAG_LENGTH);

            //返回结果
            //前面8个常量窗口期和事件个数，后面一大堆事件时间和索引
            //[窗口大小[4]，事件个数[4]，事件时间[4]，事件索引[1]，事件时间[4]，事件索引[1]，事件时间[4]，事件索引[1]...
            state.setSlice(newSlice);

        }

    }

    @OutputFunction(StandardTypes.INTEGER)//返回值类型
    public static void output(SliceState state, BlockBuilder out){
        //获取状态数据
        Slice slice = state.getSlice();
        //判断是否为空
        if (null == slice){
            out.writeInt(0);
            out.closeEntry();
            return;
        }
        //如果数据不为空，开始计算用户深度
        //添加一个临时变量
        boolean is_a = false;
        //为了保存时间，后面进行排序，创建一个数组，用于排序
        //数据：(ctime1,ctiime2,ctime3)
        List<Integer> time_array = new ArrayList<>();
        //为了后续根据事件时间获取时间索引，创建map集合
        //数据：[ctime1,evenIndex]...
        Map<Integer,Byte> timeEventMap = new HashMap<>();

        //获取事件数据的索引，使用循环方式，循环所有的事件数据
        //从第八个字节开始，因为前八个已经存了变量，后面才是事件事件
        //每次固定加 5，格式是：事件时间[4] + 事件索引[1]
        for (int index = COUNT_FLAG_LENGTH; index < slice.length(); index += COUNT_ONE_LENGTH) {
            //获取事件的时间戳和对应事件
            int timestamp = slice.getInt(index);
            byte event = slice.getByte(index + 4);

            //用于更新临时变量
            //如果event等于0 表示是第一个事件，也就是开始事件，如果没有0，证明就没有事件，直接返回即可
            //同时，如果等0 表示是一个开始完整事件，就是从头开始的
            if ((!is_a) && event ==0){
                is_a = true;
            }
            //将数据和map赋值
            time_array.add(timestamp);
            timeEventMap.put(timestamp,event);
        }

        //根据临时变量进行判断每个用户数据是否从头开始，如果不是，该用户数据就作废
        //判断每个用户数据是否从0开始
        if (!is_a){
            out.writeInt(0);
            out.closeEntry();
            return;
        }

        //接下来根据数组的事件时间，进行数据排序
        //正序排序，从小到大
        Collections.sort(time_array);

        //拿到slice的常量数据，也就是常量的事件个数和窗口期
        int windows = slice.getInt(0);
        int event_count = slice.getInt(4);

        //开始遍历所有用户的时间戳数据，并通过时间戳数据构成结果
        int maxEventIndex = 0 ;//定义最大事件的深度
        //定义集合用于内部取值
        List<int[]> temp = new ArrayList<>();

        //循环事件时间及事件类型
        for (int timestamp : time_array){
            //通过事件时间获取事件index
            Byte event = timeEventMap.get(timestamp);
            if (event == 0){
                //新建临时变量，存放的事件时间戳和事件索引
                //用来判断后续的事件index循环使用
                int [] flag = {timestamp,event};
                temp.add(flag);
            }else{
                //如果不等于0，就更新这个数组，遍历所有的数据
                for (int i = temp.size() -1; i >= 0; -- i) {
                    int[] flag  = temp.get(i);
                    if ((timestamp - flag[0]) >= windows){
                        //当前事件时间减去flag[0]时间超过窗口期，不合法，直接跳过
                        break;
                    }//判断下一次不等于0的event索引，是否是下一个事件触发的事件顺序
                    else if (event ==(flag[1] + 1)){
                        //如果小于窗口期，数据合法
                        flag[1] = event;
                        //如果当前的最大变量的深度，小于数据的事件深度，还需要赋值
                        if (maxEventIndex < event){
                            //将这次循环的eventIndex赋值给最大深度
                            maxEventIndex = event;
                        }
                        break;
                    }
                }
                //如果循环一次直接满足最大深度，就不需要循环，直接跳出即可
                if ((maxEventIndex + 1) == event_count){
                    break;
                }
            }
        }

        //返回结果
        out.writeInt(maxEventIndex + 1);
        out.closeEntry();
    }

}
