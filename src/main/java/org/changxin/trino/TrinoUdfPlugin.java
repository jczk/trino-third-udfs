package org.changxin.trino;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.Plugin;
import org.changxin.trino.aggregation.SumDouble;
import org.changxin.trino.funnel.Funnel;
import org.changxin.trino.funnel.FunnelMerge;
import org.changxin.trino.scalar.HiveFunctions;

import java.util.Set;

public class TrinoUdfPlugin implements Plugin {

    @Override
    public Set<Class<?>> getFunctions() {
        return ImmutableSet.<Class<?>>builder()
                //添加插件class
                .add(HiveFunctions.class)
                .add(SumDouble.class)
                .add(Funnel.class)
                .add(FunnelMerge.class)
                .build();
    }
}
