package org.changxin.trino.state;

import io.trino.operator.aggregation.state.InitialBooleanValue;
import io.trino.operator.aggregation.state.NullableDoubleStateSerializer;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;
import io.trino.spi.type.Type;

@AccumulatorStateMetadata(stateSerializerClass = NullableDoubleStateSerializer.class)
public interface NullableDoubleState extends AccumulatorState {

    double getDouble();
    void setDouble(double value);

    @InitialBooleanValue(true)
    boolean isNull();

    void setNull(boolean value);

    static void write(Type type, NullableDoubleState state, BlockBuilder out){
        if (state.isNull()){
            out.appendNull();
        }else{
            type.writeDouble(out,state.getDouble());
        }
    }
}
