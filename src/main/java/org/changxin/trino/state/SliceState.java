package org.changxin.trino.state;

import io.airlift.slice.Slice;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.type.Type;

/**
 * 定义SliceState 存储状态数据
 */
public interface SliceState extends AccumulatorState {

    Slice getSlice();

    void setSlice(Slice value);

    static void write(Type type, SliceState state, BlockBuilder out){
        if (state.getSlice() == null){
            out.appendNull();
        }else{
            type.writeSlice(out,state.getSlice());
        }
    }
}
