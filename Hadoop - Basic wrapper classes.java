/*
Since files in HDFS have to be serialized and deserialized, Writable (Wrapper) classes are required in Hadoop.
So, in this file the basic of Hadoop wrapper classes are reviewed.
notice: before run this file you need to change the filename to Basic_wrapper.java

Primitive Data Type / Wrapper Class
    byte	            Byte
    short	            Short
    int	                Integer
    long	            Long
    float	            Float
    double	            Double
    boolean	            Boolean
    char	            Character

credit: https://www.w3schools.com/java/java_wrapper_classes.asp
*/

import org.apache.hadoop.io.*;
import java.util.*;

public class Basic_wrapper {


    public static void main(String[] args) {
        System.out.println("Basic Hadoop Wrapper classes and return values \n");

        IntWritable hd_int = new IntWritable(5);
        IntWritable hd_int2 = new IntWritable(1234);
            // equivalent to "Integer java_int = new Integer(5);" in native java


        DoubleWritable hd_dou = new DoubleWritable(11.1213);
            // equivalent to "Double java_dou = new Double(11,1213);" in native java

        BooleanWritable hd_bool = new BooleanWritable(true);
            // equivalent to "Boolean java_bool = new Boolean(true);" in native java

        ByteWritable hd_byte = new ByteWritable((byte)0101);
            // Byte java_byte = new Byte((byte)0101);


        // .get() use to return the value of the XXXWritable
        System.out.println(hd_int.getClass() + ", " + hd_int.get());
        System.out.println(hd_dou.getClass() + ", " + hd_dou.get());
        System.out.println(hd_bool.getClass() + ", " + hd_bool.get());
        System.out.println(hd_byte.getClass() + ", " + hd_byte.get());
        hd_int.set(1000);
        System.out.println(hd_int + ", " + hd_int.get());

        // Array of Writable
        ArrayWritable hd_arr = new ArrayWritable(IntWritable.class, new Writable[] {
                                                    new IntWritable(1),
                                                    new IntWritable(1),
                                                    hd_int});

//      ###########################
//      #       Output            #
//      ###########################
//
//      class org.apache.hadoop.io.IntWritable, 5
//      class org.apache.hadoop.io.DoubleWritable, 11.1213
//      class org.apache.hadoop.io.BooleanWritable, true
//      class org.apache.hadoop.io.ByteWritable, 65
//      class org.apache.hadoop.io.IntWritable, 1000
    }

}