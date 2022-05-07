package org.changxin.trino;


import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Test;

/**
 * Trino使用操作：
 *  1、开辟内存空间大小
 *  2、合理设置存入数据大小，保证别越界，超出内存
 *  3、内存地址结合使用
 */
public class AppTest {

    @Test
    public void allocate(){
        //设置一块内存区域，大小为10个字节大小
//        Slice slice = Slices.allocate(10);
        Slice slice = Slices.allocate(17);
        //表示第0个字节设置为100
        //一个字节等于8bit（2的8次方=256）
        slice.setByte(0,100);
//        slice.setByte(0,257);
        //获取第0个字节数据
        System.out.println(slice.getByte(0));

        //设置为int的为4个字节，如果超出内存限定，就会溢出内存，并且设置字节的值就可能不准
        //所以设置内存区域字节大小要计算长度，不能超出该有的长度
        slice.setInt(1,1024*1024*1024);
        System.out.println(slice.getInt(1));

        //报错：java.lang.IndexOutOfBoundsException: end index (11) must not be greater than size (10)
        //每个中文占三个字节（UTF-8），也就是占了6个字节
        slice.setBytes(5,"张三".getBytes());
        System.out.println(new String(slice.getBytes(5,6)));
        //内存复写
        slice.setBytes(8,Slices.utf8Slice("五"));
        //获取内存中的字符
        System.out.println(new String(slice.getBytes(5,6)));
        //相当于添加内存字符进行截取，假如添加的字符过大，可以在原有的复写过程中进行截取
        slice.setBytes(8,Slices.utf8Slice("六六"),0,6);
        System.out.println(new String(slice.getBytes(),5,9));

        //将两个不同的内存地址结合使用
        Slice allocate = Slices.allocate(12);
        slice.getBytes(5,allocate);
        System.out.println(allocate.toStringUtf8());

    }

}
