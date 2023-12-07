package com.imooc.flink.source;

import com.imooc.flink.transformation.Access;
import org.apache.flink.shaded.curator4.com.google.common.base.Strings;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class AccessSource implements SourceFunction<Access> {
    boolean running = true;

    @Override
    public void run(SourceContext<Access> ctx) throws Exception {
        Random random =new Random();
        String[] domains = {"imooc.com","a.com","b.com"};
        while(running){
            for(int i=0;i<10;i++){
                Access access=new Access();
                access.setTime(1234567L);
                access.setDomain(domains[random.nextInt(domains.length)]);
                access.setTraffic(random.nextDouble()+1000);
                ctx.collect(access);

            }
            Thread.sleep(5000);

        }

    }

    @Override
    public void cancel() {

    }
}
